"""Application-layer protocol & TCP framing (Owner: TV1 - Nhom truong).

Wire format (headers terminate at '\\n' so payload '|' never breaks framing):
  UDP presence : HELLO|<name>|<tcp_port>|<status>
  TCP identity : PEER|<name>|<tcp_port>\\n
  TCP message  : MSG|<length>\\n<exact length UTF-8 bytes>
  TCP file     : FILE|<filename>|<size_in_bytes>\\n<exact raw file bytes>
  TCP leave    : BYE|\\n
"""

from __future__ import annotations

import os
import socket
import threading
from typing import Iterator, Optional

from state import VALID_STATUSES

DEFAULT_UDP_PORT = 9999
DEFAULT_TCP_PORT = 5000
PRESENCE_INTERVAL_SEC = 3.0
PEER_TIMEOUT_SEC = 10.0
FILE_CHUNK_SIZE = 65536
MAX_HEADER_BYTES = 4096


class ProtocolError(Exception):
    """Raised when an incoming frame header violates the protocol format."""


def _sanitize_header_field(value: str, fallback: str = "_") -> str:
    cleaned = value.replace("\r", "").replace("\n", "").replace("|", "_").strip()
    return cleaned or fallback


def _parse_port(raw: str) -> int:
    port = int(raw)
    if not (1 <= port <= 65535):
        raise ValueError(f"Port out of range: {port}")
    return port


def encode_hello(name: str, tcp_port: int, status: str) -> bytes:
    """Encode UDP presence packet HELLO|<name>|<tcp_port>|<status>."""
    if status not in VALID_STATUSES:
        raise ProtocolError(f"Invalid status for HELLO: {status!r}")
    if not (1 <= tcp_port <= 65535):
        raise ProtocolError(f"Invalid TCP port for HELLO: {tcp_port}")
    safe_name = _sanitize_header_field(name)
    return f"HELLO|{safe_name}|{tcp_port}|{status}".encode("utf-8")


def parse_hello(data: bytes) -> Optional[tuple[str, int, str]]:
    """Parse UDP presence packet. Returns (name, tcp_port, status) or None if invalid."""
    try:
        text = data.decode("utf-8", errors="strict").strip()
        parts = text.split("|")
        if len(parts) != 4 or parts[0] != "HELLO":
            return None
        name = parts[1].strip()
        tcp_port = _parse_port(parts[2])
        status = parts[3].strip()
        if not name or status not in VALID_STATUSES:
            return None
        return name, tcp_port, status
    except (UnicodeDecodeError, ValueError):
        return None


def encode_peer(name: str, tcp_port: int) -> bytes:
    """Encode TCP handshake frame PEER|<name>|<tcp_port>\\n."""
    if not (1 <= tcp_port <= 65535):
        raise ProtocolError(f"Invalid TCP port for PEER: {tcp_port}")
    safe_name = _sanitize_header_field(name)
    return f"PEER|{safe_name}|{tcp_port}\n".encode("utf-8")


def encode_msg(text: str) -> bytes:
    """Encode length-prefixed UTF-8 chat message MSG|<length>\\n<utf-8 bytes>."""
    payload = text.encode("utf-8")
    header = f"MSG|{len(payload)}\n".encode("utf-8")
    return header + payload


def encode_file_header(filename: str, size_bytes: int) -> bytes:
    """Encode FILE|<basename>|<size_in_bytes>\\n header preceding raw file bytes."""
    if size_bytes < 0:
        raise ProtocolError(f"Negative file size: {size_bytes}")
    base = os.path.basename(filename.strip())
    safe_name = _sanitize_header_field(base, fallback="received_file")
    return f"FILE|{safe_name}|{size_bytes}\n".encode("utf-8")


def encode_bye() -> bytes:
    """Encode BYE|\\n frame to close the active chat cleanly."""
    return b"BYE|\n"


def send_frame(
    sock: socket.socket,
    frame: bytes,
    send_lock: Optional[threading.Lock] = None,
) -> None:
    """Send complete frame bytes over TCP, optionally guarded by send_lock."""
    if send_lock is None:
        sock.sendall(frame)
        return
    with send_lock:
        sock.sendall(frame)


def recv_line(sock: socket.socket, max_bytes: int = MAX_HEADER_BYTES) -> bytes:
    """Read one '\\n'-terminated header line without consuming following payload bytes."""
    buf = bytearray()
    while True:
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("Peer closed connection while reading frame header.")
        if chunk == b"\n":
            return bytes(buf).rstrip(b"\r")
        buf.extend(chunk)
        if len(buf) > max_bytes:
            raise ProtocolError(f"Frame header exceeded {max_bytes} bytes.")


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from a TCP stream; raise ConnectionError on premature EOF."""
    if n < 0:
        raise ProtocolError(f"Negative read length: {n}")
    if n == 0:
        return b""

    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError(
                f"Connection closed after {n - remaining}/{n} expected bytes."
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def iter_recv_chunks(
    sock: socket.socket,
    total_size: int,
    chunk_size: int = FILE_CHUNK_SIZE,
) -> Iterator[bytes]:
    """Stream exact total_size bytes in chunks so large files do not fill RAM."""
    if total_size < 0:
        raise ProtocolError(f"Negative total stream size: {total_size}")
    remaining = total_size
    while remaining > 0:
        chunk = sock.recv(min(chunk_size, remaining))
        if not chunk:
            raise ConnectionError(
                f"File stream interrupted at {total_size - remaining}/{total_size} bytes."
            )
        remaining -= len(chunk)
        yield chunk


def read_frame_header(sock: socket.socket) -> tuple[str, tuple]:
    """Read and parse the next TCP frame header into (tag, fields)."""
    raw_line = recv_line(sock)
    try:
        line = raw_line.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ProtocolError("Header is not valid UTF-8.") from exc

    parts = line.split("|")
    tag = parts[0]

    try:
        if tag == "PEER" and len(parts) == 3:
            name = parts[1].strip()
            if not name:
                raise ProtocolError("Empty peer name in PEER header.")
            return "PEER", (name, _parse_port(parts[2]))

        if tag == "MSG" and len(parts) == 2:
            length = int(parts[1])
            if length < 0:
                raise ProtocolError(f"Negative MSG length: {length}")
            return "MSG", (length,)

        if tag == "FILE" and len(parts) == 3:
            filename = parts[1].strip()
            size_bytes = int(parts[2])
            if not filename or size_bytes < 0:
                raise ProtocolError("Invalid FILE header metadata.")
            return "FILE", (filename, size_bytes)

        if tag == "BYE" and len(parts) == 2:
            return "BYE", ()
    except ValueError as exc:
        raise ProtocolError(f"Invalid numeric field in header: {line!r}") from exc

    raise ProtocolError(f"Malformed frame header: {line!r}")
