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
| `EXPIRED_RETENTION_DAYS` | `30` | 1..=3650; retain expired history after expiration, then delete the row and file metadata |
| `APP_CLEANUP_INTERVAL_SECS` | `60` | |
| `MAX_CONCURRENT_DOWNLOADS` | `3` | extra downloads stay `pending_metadata` |
| `MAX_DOWNLOAD_TASKS` | `50` | 1..=10000 queued + running downloads; full queue returns 429 before creating a row, directory or task |
| `MAX_TORRENT_FILES` | `10000` | 1..=100000; checked in metadata-only preflight before payload files are created |
| `ALLOW_HTTP_TRACKERS` | `false` | strip HTTP(S) trackers by default; `true` opts into redirect/unbounded-response risks and logs a startup warning |
| `TORRENT_SOCKS_PROXY_URL` | unset | librqbit SOCKS proxy URL, e.g. `socks5://127.0.0.1:1080`; use a filtering proxy and enforce network policy too |
| `METADATA_TIMEOUT_SECS` | `600` | |
| `NO_PROGRESS_TIMEOUT_SECS` | `600` | |
| `MAX_TORRENT_BYTES` | unset | reject larger torrents in metadata-only preflight before payload files/download start |
| `TRACKER_ALLOWLIST` | unset | comma-separated tracker hosts; all others dropped |

## Magnet handling

`so=` (selective download) is rejected with 400; magnets are limited to 4096 bytes.
`tr=` trackers with non-http/https/udp schemes or pointing to loopback, private,
link-local, unique-local or otherwise non-public addresses (hostnames are
resolved) are dropped. Duplicate info-hashes return the existing active or
unexpired completed download (`200` instead of `202`).
HTTP(S) trackers are additionally stripped before librqbit receives the magnet,
unless `ALLOW_HTTP_TRACKERS=true`. This can reduce discovery for torrents that
depend on HTTP-only trackers; UDP trackers and DHT remain available.

## Network egress

The service connects to peers and trackers chosen by API clients (and DHT).
Tracker filtering is best-effort (DNS can change between check and use), and
peer addresses are not filtered. Restrict egress at the network level
(firewall / container network policy) so the container cannot reach internal
networks and cloud metadata endpoints (e.g. 169.254.169.254).

librqbit 8.1.1 constructs its own HTTP client: there is no public client injection,
redirect-policy or response-size-limit hook. Its `disable_trackers` option is
declared but unused in this version. The SOCKS option is supported, but is not
a substitute for restricting direct UDP/DHT egress. When HTTP trackers are opted
in, redirects can reach internal hosts and tracker responses are buffered without
a size bound. A filtering proxy/firewall must enforce destination and resource
limits; DNS pre-checks alone do not prevent rebinding. No dependency fork is used.
