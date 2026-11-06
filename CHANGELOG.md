# Changelog

## 0.1.0 (unreleased)

First version, not yet run on a full-size server.

- Streaming backup to Google Drive with no staging on disk, md5 of every stream compared with Drive.
- Local and SFTP sources, one archive per dimension, second pass for files changed during the copy.
- MariaDB dumps and ClickHouse Native export.
- Lowest-priority worker in its own container, fast zstd levels by default.
- Waiting for other backup jobs through their Drive folders.
- Discord panel with status, Run now, list of copies, log and alerts.
- Docker image, compose file, GitHub Actions for tests and releases.
