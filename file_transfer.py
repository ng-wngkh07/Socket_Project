"""TCP file transfer sender thread & disk stream receiver (Owner: Thanh vien 3).

Implements Section 3.4, 5.1, 5.2, and 5.3:
- File sender thread uses a dedicated TCP socket so chatting stays unblocked.
- Shows percentage progress while sending.
- Saves incoming files into downloads/ safely and deletes partial files on error.
"""

from __future__ import annotations

import os
import socket
import threading
from typing import Any, BinaryIO, Optional

from protocol import (
    FILE_CHUNK_SIZE,
    ProtocolError,
    encode_file_header,
    encode_peer,
    iter_recv_chunks,
    send_frame,
)
from state import Peer, SharedState


def _format_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.1f} MB"


def _sanitize_filename(filename: str) -> str:
    # Normalize both Windows and POSIX separators before taking basename to block path traversal.
    cleaned = os.path.basename(filename.replace("\\", "/").strip())
    if not cleaned or cleaned in (".", ".."):
        return "received_file"
    return cleaned


class FileTransferManager:
    """Handles outgoing background file sends and incoming streamed file saves."""

    def __init__(
        self,
        state: SharedState,
        ui: Any,
        downloads_dir: str = "downloads",
        chunk_size: int = FILE_CHUNK_SIZE,
        connect_timeout: float = 10.0,
        io_timeout: float = 30.0,
    ) -> None:
        self.state = state
        self.ui = ui
        self.downloads_dir = downloads_dir
        self.chunk_size = chunk_size
        self.connect_timeout = connect_timeout
        self.io_timeout = io_timeout
        self._path_lock = threading.Lock()

    def _safe_notify(self, text: str) -> None:
        if self.ui is not None:
            self.ui.notify(text)

    def _reserve_download_file(self, safe_name: str) -> tuple[str, BinaryIO]:
        os.makedirs(self.downloads_dir, exist_ok=True)
        stem, ext = os.path.splitext(safe_name)
        with self._path_lock:
            counter = 0
            while True:
                candidate_name = safe_name if counter == 0 else f"{stem}_{counter}{ext}"
                dest_path = os.path.join(self.downloads_dir, candidate_name)
                try:
                    file_obj = open(dest_path, "xb")
                    return dest_path, file_obj
                except FileExistsError:
                    counter += 1

    def send_file_async(self, partner: Optional[Peer], filepath: str) -> None:
        """Validate filepath and spawn a File sender thread to send over dedicated TCP."""
        if partner is None:
            self._safe_notify("[!] No active chat partner. Use /chat <name|number> first.")
            return

        clean_path = filepath.strip()
        if len(clean_path) >= 2 and clean_path[0] == clean_path[-1] and clean_path[0] in ('"', "'"):
            clean_path = clean_path[1:-1].strip()

        if not clean_path:
            self._safe_notify("[!] Usage: /send <file>")
            return

        if not os.path.isfile(clean_path):
            self._safe_notify(f"[!] File not found: {clean_path}")
            return

        filename = os.path.basename(clean_path)
        worker = threading.Thread(
            target=self._send_file_worker,
            args=(partner.copy(), clean_path, filename),
            name=f"FileSender-{filename}",
            daemon=True,
        )
        worker.start()

    def _send_file_worker(self, partner: Peer, filepath: str, filename: str) -> None:
        sock: Optional[socket.socket] = None
        try:
            with open(filepath, "rb") as file_obj:
                total_size = os.fstat(file_obj.fileno()).st_size
                size_label = _format_size(total_size)

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.connect_timeout)
                sock.connect((partner.ip, partner.tcp_port))
                sock.settimeout(self.io_timeout)

                # Dedicated TCP connection: identify peer then immediately send FILE header.
                send_frame(
                    sock,
                    encode_peer(self.state.my_name, self.state.my_tcp_port)
                    + encode_file_header(filename, total_size),
                )

                sent_bytes = 0
                last_milestone = 0
                while sent_bytes < total_size:
                    to_read = min(self.chunk_size, total_size - sent_bytes)
                    chunk = file_obj.read(to_read)
                    if not chunk:
                        raise OSError("Source file was truncated while sending.")
                    sock.sendall(chunk)
                    sent_bytes += len(chunk)

                    percent = (sent_bytes * 100) // total_size
                    milestone = (percent // 10) * 10
                    if 0 < milestone < 100 and milestone > last_milestone:
                        last_milestone = milestone
                        self._safe_notify(f"[*] Sending {filename} ({size_label}) ... {milestone}%")

                try:
                    sock.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                self._safe_notify(f"[*] Sending {filename} ({size_label}) ... 100% Done.")
        except (OSError, ConnectionError, ProtocolError) as exc:
            self._safe_notify(f"[!] Failed to send {filename} to {partner.name}: {exc}")
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    def receive_file_stream(
        self,
        sock: socket.socket,
        sender_name: str,
        filename: str,
        size_bytes: int,
    ) -> None:
        """Receive exact size_bytes into downloads/; delete partial file on interruption."""
        if size_bytes < 0:
            self._safe_notify(f"[!] Invalid file size ({size_bytes}) from {sender_name}.")
            return

        safe_name = _sanitize_filename(filename)
        size_label = _format_size(size_bytes)
        dest_path: Optional[str] = None
        file_obj: Optional[BinaryIO] = None
        completed = False

        try:
            try:
                sock.settimeout(self.io_timeout)
            except OSError:
                pass

            dest_path, file_obj = self._reserve_download_file(safe_name)
            self._safe_notify(f"[*] Receiving {safe_name} ({size_label}) from {sender_name}...")

            received_bytes = 0
            for chunk in iter_recv_chunks(sock, size_bytes, chunk_size=self.chunk_size):
                file_obj.write(chunk)
                received_bytes += len(chunk)

            if received_bytes != size_bytes:
                raise ConnectionError(
                    f"Incomplete file transfer ({received_bytes}/{size_bytes} bytes)."
                )

            file_obj.flush()
            completed = True
            self._safe_notify(
                f"[*] Received {safe_name} ({size_label}) from {sender_name} -> {dest_path}"
            )
        except (ConnectionError, ProtocolError, OSError) as exc:
            self._safe_notify(
                f"[!] File transfer failed for {safe_name} from {sender_name}: {exc}"
            )
        finally:
            if file_obj is not None:
                try:
                    file_obj.close()
                except OSError:
                    pass
            if not completed and dest_path is not None and os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except OSError:
                    pass
