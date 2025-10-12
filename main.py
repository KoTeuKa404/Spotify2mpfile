from __future__ import annotations
import csv, os, re, sys, glob, subprocess, shutil, concurrent.futures, multiprocessing, unicodedata, time
from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple, Dict
from concurrent.futures import CancelledError
try:
    from concurrent.futures.process import BrokenProcessPool
except ImportError:
    BrokenProcessPool = RuntimeError

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtWidgets import (
    QApplication, QWidget, QPushButton, QFileDialog, QLineEdit, QHBoxLayout,
    QVBoxLayout, QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, QProgressBar,
    QCheckBox
)

import yt_dlp

MAX_WORKERS = max(2, min(os.cpu_count() or 4, 4))

FFMPEG_EXE = None
FFMPEG_DIR = None
try:
    import imageio_ffmpeg as iio_ffmpeg
    FFMPEG_EXE = iio_ffmpeg.get_ffmpeg_exe()
    if FFMPEG_EXE and os.path.isfile(FFMPEG_EXE):
        FFMPEG_DIR = os.path.dirname(FFMPEG_EXE)
        os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ.get("PATH", "")
except Exception:
    pass


@dataclass
class Track:
    title: str
    artist: str
    album: Optional[str] = None
    duration_ms: Optional[int] = None


FORBIDDEN_CHARS = set('\\/:*?"<>|')


def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKC", (name or "").strip())
    cleaned = []
    for ch in name:
        if ch in FORBIDDEN_CHARS or unicodedata.category(ch).startswith("C"):
            cleaned.append("_")
        else:
            cleaned.append(ch)
    name = "".join(cleaned)
    name = re.sub(r"\s+", " ", name).rstrip(". ")
    base_upper = name.split(".")[0].upper()
    if base_upper in {"CON", "PRN", "AUX", "NUL"} or re.fullmatch(r"(COM|LPT)[1-9]", base_upper):
        name = "_" + name
    return name[:180]


def parse_spotify_csv(path: str) -> List[Track]:
    tracks: List[Track] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            f.seek(0)
            reader2 = csv.reader(f)
            for row in reader2:
                if len(row) >= 2:
                    tracks.append(Track(title=row[0], artist=row[1]))
            return tracks

        fields = {(name or "").strip().lower(): (name or "") for name in reader.fieldnames}
        title_key = artist_key = album_key = duration_key = None
        for k_lower, original in fields.items():
            if title_key is None and ("track name" in k_lower or k_lower == "title"):
                title_key = original
            if artist_key is None and ("artist" in k_lower):
                artist_key = original
            if album_key is None and ("album" in k_lower):
                album_key = original
            if duration_key is None and ("duration" in k_lower and "ms" in k_lower):
                duration_key = original

        for row in reader:
            title = (row.get(title_key) or "").strip()
            artist = (row.get(artist_key) or "").strip()
            album = (row.get(album_key) or "").strip() if album_key else None
            dur = None
            if duration_key:
                try:
                    dur = int((row.get(duration_key) or "0").strip())
                except Exception:
                    dur = None
            if title and artist:
                tracks.append(Track(title=title, artist=artist, album=album or None, duration_ms=dur))
    return tracks


def _popen_silent(cmd: list[str]) -> subprocess.Popen:
    startupinfo = None
    creationflags = 0
    if os.name == "nt":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        creationflags = subprocess.CREATE_NO_WINDOW
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
        startupinfo=startupinfo, creationflags=creationflags
    )


def hard_convert_to_mp3_proc(in_path: str, out_path: str, kill_event) -> bool:
    """ffmpeg з можливістю жорсткої зупинки через kill_event."""
    ffmpeg = FFMPEG_EXE or shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")
    if not ffmpeg:
        return False
    cmd = [
        ffmpeg, "-hide_banner", "-loglevel", "error", "-y",
        "-i", in_path, "-vn",
        "-codec:a", "libmp3lame", "-q:a", "4", "-ar", "44100", "-ac", "2",
        out_path
    ]
    try:
        p = _popen_silent(cmd)
        while True:
            if kill_event.is_set():
                try: p.terminate()
                except Exception: pass
                try: p.kill()
                except Exception: pass
                return False
            rc = p.poll()
            if rc is not None:
                break
            time.sleep(0.05)
        ok = (p.returncode == 0)
        return ok and os.path.exists(out_path) and os.path.getsize(out_path) > 0
    except Exception:
        return False


def process_one(index: int, t: Dict, out_dir: str, embed_metadata: bool, kill_event) -> Tuple[int, bool, str, str]:
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    os.environ.setdefault("YTDLP_ENCODING", "utf-8")

    if yt_dlp is None or not FFMPEG_EXE:
        return index, False, "yt-dlp or ffmpeg are not available", ""

    title = (t.get("title") or "").strip()
    artist = (t.get("artist") or "").strip()
    duration_ms = t.get("duration_ms", None)
    basename = sanitize_filename(f"{artist} - {title}")

    try:
        target_sec = (int(duration_ms) // 1000) if duration_ms else None
        query = f"ytsearch3:{artist} - {title}"

        def build_opts():
            outtmpl = os.path.join(out_dir, f"{basename}.%(ext)s")
            return {
                "outtmpl": outtmpl,
                "noprogress": True,
                "quiet": True,
                "ignoreerrors": True,
                "noplaylist": True,
                "format": "bestaudio/best",
                "postprocessors": [],
                "prefer_ffmpeg": True,
                "ffmpeg_location": FFMPEG_DIR,
                "retries": 5,
                "fragment_retries": 5,
                "continuedl": True,
                "concurrent_fragment_downloads": 1,
                "socket_timeout": 30,
                "http_chunk_size": 1_048_576,
                "throttledratelimit": 0,
            }

        def hook(d):
            if kill_event.is_set():
                raise yt_dlp.utils.DownloadError("killed by user")

        opts = build_opts()
        opts["progress_hooks"] = [hook]

        if kill_event.is_set():
            return index, False, "Canceled", ""

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(query, download=False)
            entries = info.get("entries") or []
            if not entries:
                return index, False, "No results found", ""
            chosen = entries[0]
            if target_sec:
                best, best_diff = None, 10 ** 9
                for e in entries:
                    dur = e.get("duration")
                    if dur is None:
                        continue
                    diff = abs(dur - target_sec)
                    if 0.85 * target_sec <= dur <= 1.15 * target_sec and diff < best_diff:
                        best, best_diff = e, diff
                chosen = best or chosen

            if kill_event.is_set():
                return index, False, "Canceled", ""

            url = chosen.get("webpage_url") or chosen.get("url")
            if not url:
                return index, False, "Unknown link", ""

            with yt_dlp.YoutubeDL(opts) as ydl2:
                ydl2.download([url])

        if kill_event.is_set():
            return index, False, "Canceled", ""

        pattern = os.path.join(out_dir, glob.escape(basename) + ".*")
        candidates = [p for p in glob.glob(pattern) if not p.lower().endswith(".mp3")]
        if not candidates:
            return index, False, "Uploaded file not found", ""
        src_file = candidates[0]
        mp3_path = os.path.join(out_dir, f"{basename}.mp3")

        ok = hard_convert_to_mp3_proc(src_file, mp3_path, kill_event)
        if not ok:
            try: os.remove(mp3_path)
            except Exception: pass
            return index, False, "FFmpeg conversion to MP3 failed/canceled", ""

        try:
            os.remove(src_file)
        except Exception:
            pass

        if kill_event.is_set():
            try: os.remove(mp3_path)
            except Exception: pass
            return index, False, "Canceled", ""

        return index, True, "Done", mp3_path

    except yt_dlp.utils.DownloadError as e:
        try:
            part_glob = os.path.join(out_dir, basename + ".*.part")
            for p in glob.glob(part_glob):
                try: os.remove(p)
                except Exception: pass
        except Exception:
            pass
        if "killed" in str(e).lower():
            return index, False, "Canceled", ""
        return index, False, f"Error: {e}", ""
    except Exception as e:
        return index, False, f"Error: {e}", ""


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV → MP3 Downloader (hard stop)")
        self.resize(980, 600)

        self.csv_path_edit = QLineEdit()
        self.btn_browse_csv = QPushButton("Select CSV")
        self.btn_browse_csv.clicked.connect(self.choose_csv)
        self.out_dir_edit = QLineEdit()
        self.btn_browse_out = QPushButton("Select folder")
        self.btn_browse_out.clicked.connect(self.choose_out_dir)
        self.chk_metadata = QCheckBox("Embed metadata and cover")
        self.chk_metadata.setChecked(False)
        self.btn_load = QPushButton("Download tracks from CSV")
        self.btn_load.clicked.connect(self.load_csv)
        self.btn_start = QPushButton("Start download")
        self.btn_start.clicked.connect(self.start_download)
        self.btn_start.setEnabled(False)
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.clicked.connect(self.stop_download)
        self.btn_stop.setEnabled(False)
        self.overall_progress = QProgressBar()
        self.overall_progress.setRange(0, 100)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Name", "Artist", "Status", "File"])
        for i in [0, 1, 3]:
            self.table.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        top1 = QHBoxLayout()
        top1.addWidget(self.csv_path_edit, 1)
        top1.addWidget(self.btn_browse_csv)
        top2 = QHBoxLayout()
        top2.addWidget(self.out_dir_edit, 1)
        top2.addWidget(self.btn_browse_out)
        ctrl = QHBoxLayout()
        ctrl.addWidget(self.btn_load)
        ctrl.addStretch(1)
        ctrl.addWidget(self.btn_start)
        ctrl.addWidget(self.btn_stop)
        main = QVBoxLayout(self)
        main.addLayout(top1)
        main.addLayout(top2)
        main.addWidget(self.chk_metadata)
        main.addLayout(ctrl)
        main.addWidget(self.table, 1)
        main.addWidget(self.overall_progress)

        self.tracks: List[Track] = []
        self.futures: List[concurrent.futures.Future] = []
        self.future_to_index: Dict[concurrent.futures.Future, int] = {}
        self.executor: Optional[concurrent.futures.ProcessPoolExecutor] = None
        self.timer: Optional[QTimer] = None
        self.done_count = 0
        self.stopping = False

        self.mgr = multiprocessing.Manager()
        self.kill_event = self.mgr.Event()

    def choose_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select CSV", "", "CSV Files (*.csv);;All Files (*)")
        if path:
            self.csv_path_edit.setText(path)

    def choose_out_dir(self):
        path = QFileDialog.getExistingDirectory(self, "Select output folder", "")
        if path:
            self.out_dir_edit.setText(path)

    def load_csv(self):
        path = self.csv_path_edit.text().strip()
        if not path or not os.path.exists(path):
            QMessageBox.warning(self, "Error", "Please select a valid path to the CSV file.")
            return
        try:
            self.tracks = parse_spotify_csv(path)
        except Exception as e:
            QMessageBox.critical(self, "CSV reading error", str(e))
            return
        self.table.setRowCount(0)
        for t in self.tracks:
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QTableWidgetItem(t.title))
            self.table.setItem(r, 1, QTableWidgetItem(t.artist))
            self.table.setItem(r, 2, QTableWidgetItem("Waiting"))
            self.table.setItem(r, 3, QTableWidgetItem(""))
        self.btn_start.setEnabled(bool(self.tracks))

    def start_download(self):
        out_dir = self.out_dir_edit.text().strip()
        if not out_dir:
            QMessageBox.warning(self, "Error", "Specify the output folder.")
            return
        os.makedirs(out_dir, exist_ok=True)
        embed = self.chk_metadata.isChecked()

        self.stopping = False
        self.kill_event.clear()

        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS)
        self.futures.clear()
        self.future_to_index.clear()
        self.done_count = 0

        for idx, t in enumerate(self.tracks):
            self.table.setItem(idx, 2, QTableWidgetItem("At work"))
            fut = self.executor.submit(process_one, idx, asdict(t), out_dir, embed, self.kill_event)
            self.futures.append(fut)
            self.future_to_index[fut] = idx

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._poll_futures)
        self.timer.start(200)
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)

    def stop_download(self):
        self.stopping = True
        self.kill_event.set()

        for fut in list(self.futures):
            fut.cancel()
        if self.executor:
            try:
                self.executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self.executor = None

        for row in range(self.table.rowCount()):
            it = self.table.item(row, 2)
            if it and it.text() not in ("Done", "Canceled"):
                self.table.setItem(row, 2, QTableWidgetItem("Canceled"))

        if self.timer:
            self.timer.stop()
            self.timer.deleteLater()
            self.timer = None

        self.futures.clear()
        self.future_to_index.clear()
        self.btn_stop.setEnabled(False)
        self.btn_start.setEnabled(True)
        self.overall_progress.setValue(0)

    def _poll_futures(self):
        if not self.futures:
            return
        for fut in list(self.futures):
            if not fut.done():
                continue
            self.futures.remove(fut)
            idx = self.future_to_index.pop(fut, None)
            try:
                r_idx, ok, status, outpath = fut.result()
                idx = r_idx if idx is None else idx
            except (CancelledError, BrokenProcessPool):
                ok, status, outpath = False, "Canceled", ""
            except Exception as e:
                ok, status, outpath = False, f"Error: {e}", ""
            if idx is not None:
                self.table.setItem(idx, 2, QTableWidgetItem(status))
                if ok and outpath:
                    self.table.setItem(idx, 3, QTableWidgetItem(outpath))
            self.done_count += 1

        total_rows = self.table.rowCount()
        if total_rows > 0:
            self.overall_progress.setValue(int(100 * self.done_count / total_rows))

        if not self.futures and not self.stopping:
            if self.timer:
                self.timer.stop()
                self.timer.deleteLater()
                self.timer = None
            if self.executor:
                try:
                    self.executor.shutdown(cancel_futures=False)
                except Exception:
                    pass
                self.executor = None
            self.btn_start.setEnabled(True)
            self.btn_stop.setEnabled(False)
            ok_cnt = sum(
                1 for i in range(self.table.rowCount())
                if self.table.item(i, 2) and "Done" in self.table.item(i, 2).text()
            )
            fail_cnt = self.table.rowCount() - ok_cnt
            QMessageBox.information(self, "Done", f"Successfully: {ok_cnt}\nErrors/Canceled: {fail_cnt}")

    def closeEvent(self, event):
        try:
            self.stopping = True
            self.kill_event.set()
            if self.timer:
                self.timer.stop()
                self.timer.deleteLater()
                self.timer = None
            if self.executor:
                try:
                    self.executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                self.executor = None
            self.futures.clear()
            self.future_to_index.clear()
        except Exception:
            pass
        event.accept()


def main():
    multiprocessing.freeze_support()
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    code = app.exec()
    os._exit(code) 


if __name__ == "__main__":
    sys.exit(main())
