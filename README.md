# WeeWX FTP Upload Optimizer

A custom threaded FTP upload script for [WeeWX](http://www.weewx.com/), designed to significantly speed up report and image uploads from your weather station to a remote server.

This project modifies the default WeeWX FTP report generator to support multi-threaded uploads, reducing total upload time, especially for large sets of images and HTML reports.

## Features

* **Multi-threaded FTP uploads** using Python `concurrent.futures.ThreadPoolExecutor`.
* Compatible with WeeWX report structure (`.html`, `.png`, `.xml`, etc.).
* Easy configuration for server credentials and remote directories.
* Maintains full compatibility with default WeeWX reporting.

## Speedup Statistics

Tested on a Lubuntu VM running on ProxMox on an i5-8500 with 4GB RAM and two cores allocated, with a run of ~ 36 files:

| Version    | Upload Method                  | Threads | Total Files | Total Time |
| ---------- | ------------------------------ | ------- | ----------- | ---------- |
| Original   | Sequential                     | 1       | 36          | 10.42 s    |
| Optimized  | Threaded                       | 4       | 38          | 7.38 s     |
| Optimized  | Threaded                       | 8       | 36          | 5.97 s     |

> **Note:** Threaded uploads yield up to ~2× speedup in practice, depending on network and server latency.

## Installation

1. Ensure you have a backup of your original ftpupload.py, located at `/usr/share/weewx/weeutil/ftpupload.py` in case something goes wrong. Open `ftpupload.py` in your favorite text editor and simply paste the contents of this new and improved ftpupload.py.

2. To modify thread_count, per_file_retries or retry_backoff, feel free to experiment with what works best for you:

```ini
                 ciphers=None,
                 thread_count=8,
                 per_file_retries=2,
                 retry_backoff=2.0
```

## Usage

The script runs automatically as part of your WeeWX reports. No additional manual steps are required. The threaded version will split uploads across the number of workers specified in the config.


## Example Log Output

```text
INFO weeutil.ftpupload: Starting threaded upload with 8 workers...
DEBUG weeutil.ftpupload: Uploaded /var/www/html/weewx/dayrain.png -> /wx/dayrain.png
DEBUG weeutil.ftpupload: Uploaded /var/www/html/weewx/daybarometer.png -> /wx/daybarometer.png
...
INFO weewx.reportengine: ftpgenerator: Ftp'd 36 files in 5.97 seconds
```

## Contributing

Contributions welcome!


## License

MIT License – see `LICENSE` file.

