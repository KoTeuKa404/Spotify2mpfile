# CSV → MP3 Downloader (PyQt + yt-dlp, silent ffmpeg, multiprocess, Unicode-safe)

A tiny desktop app that reads a **Spotify playlist CSV** and downloads each track as **MP3**.
It searches the track on **YouTube** (via `yt-dlp`), downloads best audio, and **silently** converts to `.mp3` using `ffmpeg` from `imageio-ffmpeg` (no PATH needed, no console windows).

## Highlights
- **CSV in → MP3 out** (YouTube as the audio source)
- **No ffmpeg on PATH required** (bundled through `imageio-ffmpeg`)
- **Silent** conversion (no pop-up consoles)
- **Multiprocessing**: download/convert multiple tracks in parallel
- **Unicode-safe**: Cyrillic, CJK (Chinese/Japanese/Korean), accents, emoji in names
- Optional **metadata embedding** (cover/tags)

## 1) Getting a Spotify CSV (Exportify)
Export your Spotify playlist to CSV using **Exportify** (web tool for exporting Spotify playlists).
The CSV typically includes columns like `Track Name`, `Artist`, `Album`, and `Duration (ms)`.

> Only `Track Name` and `Artist` are strictly required. `Duration (ms)` is optional but helps match the correct video.

## 2) Install (dev environment)
```bash
pip install PyQt6 yt-dlp imageio-ffmpeg
```

> `imageio-ffmpeg` downloads a compatible ffmpeg binary automatically the first time.

## 3) Run
```bash
python main.py
```
**Steps:**
1. Choose your Spotify CSV.
2. Select the output folder.
3. Optionally tick “Embed metadata & cover”.
4. Click “Start”.

## 4) Build to EXE (Windows)
```bash
pip install pyinstaller
pyinstaller --noconfirm --onefile --windowed --name SpotCSV2MP3 ^
  --collect-data imageio_ffmpeg ^
  main.py
```

The build works standalone — ffmpeg is auto-provided by `imageio-ffmpeg`.

## 5) How it works
1. Parses the CSV into track objects.
2. Searches YouTube for `artist - title`.
3. Downloads best audio.
4. Converts to MP3 silently.
5. Optionally embeds metadata.
6. Saves `Artist - Title.mp3`.

## 6) Notes
- Multiprocess: up to 4 workers.
- Unicode-safe filenames.
- Duration matching helps accuracy.
- Silent ffmpeg (no popups).

## 7) Troubleshooting
- `.webm` instead of `.mp3`: conversion failed → reinstall `imageio-ffmpeg`.
- `NoneType` errors: yt-dlp couldn’t find results → try again later.
- Broken characters: ensure CSV is UTF-8 (Exportify uses UTF-8).

## 8) Legal
This project **does not download from Spotify** — it uses YouTube as a source based on track names
Use responsibly according to YouTube’s Terms of Service.

## 9) Tech Stack
- PyQt6 GUI
- yt-dlp downloader
- imageio-ffmpeg backend
- multiprocessing
- Unicode-safe filename sanitizer

---
Happy listening! 🎧
