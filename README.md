# Yellod Downloader

Self-hosted media downloader web app. Paste a YouTube (or other yt-dlp supported) link, pick **MP3 audio** or **MP4 video**, and manage everything from a live progress queue.

```
                                              ,,
     paste link ->  [ Download ]  ->  queue  ->  mp3
```

* Rust (axum) single binary, tiny footprint
* MP3 extraction via yt-dlp + ffmpeg — for MP3 jobs only the audio stream is fetched (no wasted video download)
* **Smart routing**: tries a direct connection first and automatically falls back to a SOCKS proxy (e.g. Tor) when a site blocks this server; learns per host, so later jobs start on the working route immediately
* Live progress over WebSocket (real %/speed/ETA for yt-dlp downloads) and instant cancel
* Batch/playlist selection, multi-link paste, one-click retry for failed jobs
* One folder per job — only the final MP3/MP4 file is kept, nothing else
* Deleting a job from history also deletes its file from disk
* Optional cookies upload for videos that require a signed-in session
* Reuses what the system already has: `yt-dlp`, `aria2c`, deno/node/bun are detected at startup (aria2c gives multi-connection speed on direct downloads)

## One-command install (Ubuntu/Debian, x86_64 & arm64)

```bash
curl -fsSL https://raw.githubusercontent.com/xyzyt010/yellod-installer/main/install.sh | sudo bash
```

The installer is a step-by-step terminal wizard: it detects your OS/arch, lets you pick which HDD/SSD (or folder) to store downloads on, downloads the prebuilt binary for your architecture, hardens the systemd service and starts it.

## Manual build

Requirements: Rust (1.75+), `ffmpeg`, [`yt-dlp`](https://github.com/yt-dlp/yt-dlp), optional `deno` (JS runtime for YouTube) and `aria2c`.

```bash
cargo build --release
./target/release/downloader
# open http://127.0.0.1:5555
```

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `DOWNLOADER_BIND_ADDRESS` | `0.0.0.0:5555` | Address/port to listen on (installer uses `127.0.0.1:5555` + tunnel/reverse proxy) |
| `DOWNLOADER_OUTPUT_DIR` | `./downloads` | Where finished files are stored (one subfolder per job) |
| `DOWNLOADER_COOKIES_PATH` | `./cookies.txt` | Netscape-format cookies file (used when present) |
| `DOWNLOADER_YTDLP_BINARY` | auto | Path to the `yt-dlp` binary |
| `DOWNLOADER_PROXY_URL` | *(empty)* | Proxy for yt-dlp, e.g. `socks5h://127.0.0.1:9050` (Tor). aria2's external downloader is skipped automatically for SOCKS proxies |
| `RUST_LOG` | `downloader=info` | Log level |

## HTTP API

| Method & path | Description |
|---|---|
| `POST /api/resolve` | `{ "url": "..." }` → title, thumbnail, formats, recommended tool |
| `POST /api/download` | `{ "url", "type": "mp3"\|"mp4", "quality", "tool"? }` → `{ job_id }` |
| `POST /api/download/batch` | `{ "items": [...] }` → `{ job_ids }` |
| `GET /api/jobs` | list jobs |
| `GET /api/jobs/{id}/file` | download the finished file (proper errors, never a fake file) |
| `DELETE /api/jobs/{id}` | delete job **and its file on disk** |
| `POST /api/jobs/delete/batch` | delete several jobs + files |
| `POST /api/upload-cookies` | upload a `cookies.txt` (stored chmod 600) |
| `GET /api/cookies` | cookies status |
| `GET /healthz` | health probe |
| `GET /ws` | WebSocket progress stream |

## Security notes

* Bind to `127.0.0.1` (default in the installer) and expose via a tunnel/reverse proxy, or put authentication in front.
* File endpoints only ever serve files that belong to finished jobs; failed jobs return a proper error (no phantom `file.txt` downloads).
* History deletion removes the file from disk, and only paths inside the configured output directory can ever be removed.
* On startup the service sweeps orphaned job folders (crash leftovers) — UUID-named folders only, never unrelated data.
* Uploaded cookies are written with `0600` permissions.

## License

MIT
