"""TCP file transfer sender thread & disk stream receiver (Owner: Thanh vien 3).

Implements Section 3.4, 5.1, 5.2, and 5.3:
- File sender thread uses a dedicated TCP socket so chatting stays unblocked.
- Shows percentage progress while sending.
- Saves incoming files into downloads/ safely and deletes partial files on error.
"""

from __future__ import annotations

import socket
from typing import Any

from state import Peer, SharedState


class FileTransferManager:
    """Handles outgoing background file sends and incoming streamed file saves."""

    def __init__(
        self,
        state: SharedState,
        ui: Any,
        downloads_dir: str = "downloads",
    ) -> None:
        self.state = state
        self.ui = ui
        self.downloads_dir = downloads_dir

    def send_file_async(self, partner: Peer, filepath: str) -> None:
        """Validate filepath and spawn a File sender thread to send over dedicated TCP."""
        raise NotImplementedError("TV3: Implement background TCP file sender with %.")

    def receive_file_stream(
        self,
        sock: socket.socket,
        sender_name: str,
        filename: str,
        size_bytes: int,
    ) -> None:
        """Receive exact size_bytes into downloads/; delete partial file on interruption."""
        raise NotImplementedError(
            "TV3: Implement streamed file save & partial-file deletion on error."
        )
