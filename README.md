# audiobooks_downloader

HTTP service that downloads torrents (by magnet URI) with librqbit and serves the files.

## Configuration

| Variable | Default | Notes |
| --- | --- | --- |
| `APP_API_KEY` | required | sent as `x-api-key` |
| `APP_BIND_ADDR` | `127.0.0.1:3000` | |
| `APP_DATABASE_URL` | `sqlite://audiobooks_downloader.db` | created on first run |
| `APP_STORAGE_ROOT` | `./storage` | holds `.lock`; one instance per root |
| `APP_DOWNLOAD_TTL_SECS` | `86400` | 1..=31536000; also retention for failed downloads |
| `APP_CLEANUP_INTERVAL_SECS` | `60` | |
| `MAX_CONCURRENT_DOWNLOADS` | `3` | extra downloads stay `pending_metadata` |
| `METADATA_TIMEOUT_SECS` | `600` | |
| `NO_PROGRESS_TIMEOUT_SECS` | `600` | |
| `MAX_TORRENT_BYTES` | unset | reject larger torrents after metadata |
| `TRACKER_ALLOWLIST` | unset | comma-separated tracker hosts; all others dropped |

## Magnet handling

`so=` (selective download) is rejected with 400; magnets are limited to 4096 bytes.
`tr=` trackers with non-http/https/udp schemes or pointing to loopback, private,
link-local, unique-local or otherwise non-public addresses (hostnames are
resolved) are dropped. Duplicate info-hashes return the existing active or
unexpired completed download (`200` instead of `202`).

## Network egress

The service connects to peers and trackers chosen by API clients (and DHT).
Tracker filtering is best-effort (DNS can change between check and use), and
peer addresses are not filtered. Restrict egress at the network level
(firewall / container network policy) so the container cannot reach internal
networks and cloud metadata endpoints (e.g. 169.254.169.254).
