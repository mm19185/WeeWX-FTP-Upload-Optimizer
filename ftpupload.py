import ftplib
import hashlib
import logging
import os
import pickle
import sys
import time
import threading
import queue

log = logging.getLogger(__name__)


class FtpUpload:
    """Uploads a directory and all its descendants to a remote server.

    Backwards-compatible API. Adds worker-based persistent-connections to
    reduce per-file connect/login overhead while preserving hash/timestamp skipping.
    """

    def __init__(self, server,
                 user, password,
                 local_root, remote_root,
                 port=21,
                 name="FTP",
                 passive=True,
                 secure=False,
                 debug=0,
                 secure_data=True,
                 reuse_ssl=False,
                 encoding='utf-8',
                 ciphers=None,
                 thread_count=8,
                 per_file_retries=2,
                 retry_backoff=2.0):
        # original parameters
        self.server = server
        self.user = user
        self.password = password
        self.local_root = os.path.normpath(local_root)
        self.remote_root = os.path.normpath(remote_root)
        self.port = port
        self.name = name
        self.passive = passive
        self.secure = secure
        self.debug = debug
        self.secure_data = secure_data
        self.reuse_ssl = reuse_ssl
        self.encoding = encoding
        self.ciphers = ciphers

        # new tuning parameters (safe defaults)
        self.thread_count = int(thread_count) if thread_count and int(thread_count) > 0 else 1
        self.per_file_retries = int(per_file_retries)
        self.retry_backoff = float(retry_backoff)

        # internal sync
        self._lock = threading.Lock()
        self._uploaded_count = 0

        if self.reuse_ssl and (sys.version_info.major < 3 or sys.version_info.minor < 6):
            raise ValueError("Reusing an SSL connection requires Python version 3.6 or greater")

    # -----------------------
    # Public run method
    # -----------------------
    def run(self):
        """Perform the actual upload.
        Returns the number of files uploaded.
        """
        # Get the timestamp and members of the last upload:
        timestamp, fileset, hashdict = self.get_last_upload()

        upload_tasks = []  # list of (local_path, remote_path, filehash)

        # Walk the local directory structure and prepare tasks
        for (dirpath, unused_dirnames, filenames) in os.walk(self.local_root):

            # relative path with leading '.' to match original behavior
            local_rel_dir_path = dirpath.replace(self.local_root, '.')
            if _skip_this_dir(local_rel_dir_path):
                continue

            # This is the absolute path to the remote directory:
            remote_dir_path = os.path.normpath(os.path.join(self.remote_root,
                                                            local_rel_dir_path))

            # Ensure remote directory exists (do once synchronously) to avoid race
            try:
                # Use a single short-lived connection to make directory (keeps workers simpler)
                tmp_ftp = self._connect()
                _make_remote_dir(tmp_ftp, remote_dir_path)
                tmp_ftp.quit()
            except Exception as e:
                log.error("Could not ensure remote directory '%s': %s", remote_dir_path, e)
                # continue walking but skip putting files in this directory onto upload queue
                continue

            # Now iterate files
            for filename in filenames:
                full_local_path = os.path.join(dirpath, filename)

                # calculate hash (may be expensive, but preserves original semantics)
                try:
                    filehash = sha256sum(full_local_path)
                except Exception as e:
                    log.warning("Could not hash %s, skipping: %s", full_local_path, e)
                    filehash = None

                if _skip_this_file(timestamp, fileset, hashdict, full_local_path, filehash):
                    continue

                full_remote_path = os.path.join(remote_dir_path, filename)
                upload_tasks.append((full_local_path, full_remote_path, filehash))

        # If no tasks, save state and return 0 (preserves original behavior)
        if not upload_tasks:
            timestamp_now = time.time()
            self.save_last_upload(timestamp_now, fileset, hashdict)
            return 0

        # Build a queue and spawn worker threads that reuse a connection per worker
        task_q = queue.Queue()
        for task in upload_tasks:
            task_q.put(task)
        # sentinel None per worker to tell them to exit
        for _ in range(self.thread_count):
            task_q.put(None)

        workers = []
        for i in range(self.thread_count):
            t = threading.Thread(target=self._worker_loop,
                                 args=(task_q, fileset, hashdict),
                                 name=f"ftp-worker-{i}",
                                 daemon=True)
            t.start()
            workers.append(t)

        # Wait for workers to finish
        for t in workers:
            t.join()

        # Save state
        timestamp = time.time()
        self.save_last_upload(timestamp, fileset, hashdict)

        return self._uploaded_count

    # -----------------------
    # Worker loop (per thread)
    # -----------------------
    def _worker_loop(self, task_q, fileset, hashdict):
        """Each worker keeps a persistent ftp connection and processes tasks until sentinel."""
        ftp = None
        try:
            ftp = self._connect()
        except Exception as e:
            log.error("Worker failed to connect initially: %s", e)
            # we'll try to connect lazily when processing tasks

        while True:
            task = task_q.get()
            if task is None:
                # sentinel -> exit
                break
            local_path, remote_path, filehash = task

            # ensure ftp connection exists, attempt to (re)connect if needed
            if ftp is None:
                try:
                    ftp = self._connect()
                except Exception as e:
                    log.error("Worker could not (re)connect for %s: %s", local_path, e)
                    # skipping this task (could re-queue, but preserve original single-pass behavior)
                    continue

            success = False
            attempt = 0
            while attempt <= self.per_file_retries and not success:
                try:
                    stor_cmd = f"STOR {remote_path}"
                    with open(local_path, 'rb') as fd:
                        ftp.storbinary(stor_cmd, fd)
                    success = True
                except ftplib.all_errors as e:
                    attempt += 1
                    log.warning("Upload attempt %d failed for '%s' -> '%s': %s",
                                attempt, local_path, remote_path, e)
                    # try to reconnect on ftp errors
                    try:
                        try:
                            ftp.quit()
                        except Exception:
                            pass
                        ftp = self._connect()
                    except Exception as conn_err:
                        log.warning("Reconnect failed after upload error: %s", conn_err)
                    if attempt <= self.per_file_retries:
                        backoff = (self.retry_backoff ** attempt)
                        time.sleep(backoff)
                except Exception as e:
                    # Non-ftplib error (e.g., file read) - log and break
                    log.error("Unexpected error uploading %s: %s", local_path, e)
                    break

            if success:
                # update shared state atomically
                with self._lock:
                    fileset.add(local_path)
                    if filehash is not None:
                        hashdict[local_path] = filehash
                    self._uploaded_count += 1
                log.debug("Uploaded %s -> %s", local_path, remote_path)
            else:
                log.error("Failed to upload %s after %d attempts", local_path, self.per_file_retries + 1)

        # clean up connection on thread exit
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass

    # -----------------------
    # Connection helper
    # -----------------------
    def _connect(self):
        """Create and return an FTP (or FTPS if requested) connection."""
        if self.secure:
            # keep old logic for FTPS compatibility (rare for you since you use plain FTP)
            try:
                ftp_server = ftplib.FTP_TLS(encoding=self.encoding)
            except TypeError:
                ftp_server = ftplib.FTP_TLS()
            if self.ciphers:
                try:
                    ftp_server.context.set_ciphers(self.ciphers)
                except Exception:
                    log.warning("Could not set FTPS ciphers; continuing without.")
        else:
            try:
                ftp_server = ftplib.FTP(encoding=self.encoding)
            except TypeError:
                ftp_server = ftplib.FTP()

        if self.debug >= 2:
            ftp_server.set_debuglevel(self.debug)

        ftp_server.set_pasv(self.passive)
        ftp_server.connect(self.server, self.port)
        ftp_server.login(self.user, self.password)
        if self.secure and self.secure_data:
            try:
                ftp_server.prot_p()
            except Exception:
                log.debug("Could not set PROT P; proceeding without protected data channel.")
        return ftp_server

    # -----------------------
    # Persistence (unchanged)
    # -----------------------
    def get_last_upload(self):
        """Reads the time and members of the last upload from the local root"""
        timestamp_file_path = os.path.join(self.local_root, "#%s.last" % self.name)
        try:
            with open(timestamp_file_path, "rb") as f:
                timestamp = pickle.load(f)
                fileset = pickle.load(f)
                hashdict = pickle.load(f)
        except (IOError, EOFError, pickle.PickleError, AttributeError):
            timestamp = 0
            fileset = set()
            hashdict = {}
            try:
                os.remove(timestamp_file_path)
            except OSError:
                pass
        return timestamp, fileset, hashdict

    def save_last_upload(self, timestamp, fileset, hashdict):
        """Saves the time and members of the last upload in the local root."""
        timestamp_file_path = os.path.join(self.local_root, "#%s.last" % self.name)
        with open(timestamp_file_path, "wb") as f:
            pickle.dump(timestamp, f)
            pickle.dump(fileset, f)
            pickle.dump(hashdict, f)


# -----------------------
# Helper functions (preserved/unchanged behavior)
# -----------------------

def _skip_this_file(timestamp, fileset, hashdict, full_local_path, filehash):
    """Determine whether to skip a specific file."""
    filename = os.path.basename(full_local_path)
    if filename[-1] == '~' or filename[0] == '#':
        return True

    if full_local_path not in fileset:
        return False

    if filehash is not None:
        # use hash if available
        if full_local_path not in hashdict:
            return False
        if hashdict[full_local_path] != filehash:
            return False
    else:
        # otherwise use file time
        if os.stat(full_local_path).st_mtime > timestamp:
            return False

    # Filename is in the set and is up-to-date.
    return True


def _skip_this_dir(local_dir):
    """Determine whether to skip a directory."""
    return os.path.basename(local_dir) in {'.svn', 'CVS', '__pycache__', '.idea', '.git'}


def _make_remote_dir(ftp_server, remote_dir_path):
    """Make a remote directory if necessary."""
    try:
        ftp_server.mkd(remote_dir_path)
    except ftplib.all_errors as e:
        # Might already exist; older servers return codes like 550 or 521
        if sys.exc_info()[0] is ftplib.error_perm:
            msg = str(e).strip()
            if msg.startswith('550') or msg.startswith('521'):
                return
        log.error("Error creating directory %s: %s", remote_dir_path, e)
        raise
    log.debug("Made directory %s", remote_dir_path)


def sha256sum(filename):
    """Efficient SHA-256 streaming hash (preserves original approach)."""
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()
