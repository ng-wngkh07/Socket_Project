"""Shared thread-safe application state (Owner: TV1 - Nhom truong).

Protects online_peers, active TCP connections, current_partner, chat_histories,
and unread_messages using a single threading.Lock per Section 5.1 of the spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import socket
import threading
import time
from typing import Optional

VALID_STATUSES = ("online", "away")


@dataclass
class Peer:
    """Represents a peer discovered on the LAN."""

    name: str
    ip: str
    tcp_port: int
    status: str = "online"
    last_seen: float = field(default_factory=time.monotonic)

    @property
    def peer_id(self) -> str:
        """Stable unique peer key across LAN and multi-instance localhost tests."""
        return f"{self.name}@{self.ip}:{self.tcp_port}"

    def copy(self) -> "Peer":
        """Return a detached snapshot so callers never mutate shared state."""
        return Peer(
            name=self.name,
            ip=self.ip,
            tcp_port=self.tcp_port,
            status=self.status,
            last_seen=self.last_seen,
        )


@dataclass
class PeerConnection:
    """Represents an active TCP chat connection and its send mutex."""

    peer: Peer
    sock: socket.socket
    send_lock: threading.Lock = field(default_factory=threading.Lock)


class SharedState:
    """Thread-safe container for all mutable P2P state shared across threads."""

    def __init__(self, my_name: str, my_tcp_port: int, my_status: str = "online") -> None:
        cleaned_name = my_name.strip()
        if not cleaned_name:
            raise ValueError("Peer name cannot be empty.")
        if not (1 <= my_tcp_port <= 65535):
            raise ValueError(f"Invalid TCP port: {my_tcp_port}")
        if my_status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {my_status}")

        self.my_name = cleaned_name
        self.my_tcp_port = my_tcp_port
        self._my_status = my_status

        self._lock = threading.Lock()
        self._online_peers: dict[str, Peer] = {}
        self._active_conns: dict[str, PeerConnection] = {}
        self._current_partner_id: Optional[str] = None
        self._unread_messages: dict[str, list[tuple[str, str]]] = {}
        self._chat_histories: dict[str, list[tuple[str, str]]] = {}

    def get_my_status(self) -> str:
        """Return local user status ('online' or 'away')."""
        with self._lock:
            return self._my_status

    def set_my_status(self, status: str) -> None:
        """Update local user status ('online' or 'away')."""
        if status not in VALID_STATUSES:
            raise ValueError(f"Status must be one of {VALID_STATUSES}, got {status!r}")
        with self._lock:
            self._my_status = status

    def upsert_peer(
        self,
        name: str,
        ip: str,
        tcp_port: int,
        status: str,
        now: Optional[float] = None,
    ) -> tuple[Peer, Optional[str]]:
        """Insert or refresh an online peer.

        Returns (peer_snapshot, event) where event is 'online', 'status_changed', or None.
        """
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid peer status: {status!r}")
        if not (1 <= tcp_port <= 65535):
            raise ValueError(f"Invalid peer TCP port: {tcp_port}")

        ts = time.monotonic() if now is None else now
        candidate = Peer(name=name, ip=ip, tcp_port=tcp_port, status=status, last_seen=ts)
        key = candidate.peer_id

        with self._lock:
            existing = self._online_peers.get(key)
            if existing is None:
                self._online_peers[key] = candidate
                return candidate.copy(), "online"

            event = "status_changed" if existing.status != status else None
            existing.status = status
            existing.last_seen = ts
            return existing.copy(), event

    def prune_expired(
        self,
        timeout_sec: float = 10.0,
        now: Optional[float] = None,
    ) -> list[Peer]:
        """Remove peers silent longer than timeout_sec and return their snapshots."""
        ts = time.monotonic() if now is None else now
        removed: list[Peer] = []
        with self._lock:
            expired_keys = [
                key for key, peer in self._online_peers.items()
                if ts - peer.last_seen > timeout_sec
            ]
            for key in expired_keys:
                removed.append(self._online_peers.pop(key).copy())
        return removed

    def _sorted_peers_locked(self) -> list[Peer]:
        return sorted(
            self._online_peers.values(),
            key=lambda p: (p.name.lower(), p.ip, p.tcp_port),
        )

    def get_online_peers_snapshot(self) -> list[Peer]:
        """Return a deterministically sorted list of online peer snapshots."""
        with self._lock:
            return [p.copy() for p in self._sorted_peers_locked()]

    def resolve_peer(self, target: str) -> Optional[Peer]:
        """Resolve a 1-based /list index or peer name into an online Peer snapshot."""
        query = target.strip()
        if not query:
            return None

        with self._lock:
            peers = self._sorted_peers_locked()
            if query.isdigit():
                idx = int(query) - 1
                if 0 <= idx < len(peers):
                    return peers[idx].copy()
                return None

            for peer in peers:
                if peer.name == query:
                    return peer.copy()
            lowered = query.lower()
            for peer in peers:
                if peer.name.lower() == lowered:
                    return peer.copy()
            return None

    def get_current_partner(self) -> Optional[Peer]:
        """Return snapshot of the active chat partner, or None."""
        with self._lock:
            if self._current_partner_id is None:
                return None
            peer = self._online_peers.get(self._current_partner_id)
            if peer is not None:
                return peer.copy()
            conn = self._active_conns.get(self._current_partner_id)
            if conn is not None:
                return conn.peer.copy()
            return None

    def get_current_partner_id(self) -> Optional[str]:
        """Return the peer_id of the active chat partner, or None."""
        with self._lock:
            return self._current_partner_id

    def set_current_partner(self, peer: Optional[Peer]) -> None:
        """Set or clear the active chat partner."""
        with self._lock:
            self._current_partner_id = None if peer is None else peer.peer_id

    def register_conn(self, peer: Peer, sock: socket.socket) -> tuple[PeerConnection, Optional[socket.socket]]:
        """Register an active TCP chat connection for peer.

        Returns (new_conn, previous_sock_to_close_outside_lock).
        """
        conn = PeerConnection(peer=peer.copy(), sock=sock)
        prev_sock: Optional[socket.socket] = None
        with self._lock:
            old = self._active_conns.get(peer.peer_id)
            if old is not None and old.sock is not sock:
                prev_sock = old.sock
            self._active_conns[peer.peer_id] = conn
        return conn, prev_sock

    def get_conn(self, peer_id: str) -> Optional[PeerConnection]:
        """Return active PeerConnection for peer_id, or None."""
        with self._lock:
            return self._active_conns.get(peer_id)

    def remove_conn(
        self,
        peer_id: str,
        expected_sock: Optional[socket.socket] = None,
    ) -> tuple[Optional[PeerConnection], bool]:
        """Remove active connection if it matches expected_sock (when given).

        Returns (removed_conn, was_current_partner).
        """
        with self._lock:
            conn = self._active_conns.get(peer_id)
            if conn is None:
                return None, False
            if expected_sock is not None and conn.sock is not expected_sock:
                return None, False

            self._active_conns.pop(peer_id, None)
            was_partner = self._current_partner_id == peer_id
            if was_partner:
                self._current_partner_id = None
            return conn, was_partner

    def push_unread(self, peer_id: str, sender_name: str, text: str) -> int:
        """Buffer an incoming message from a non-active partner and return unread count."""
        with self._lock:
            self._chat_histories.setdefault(peer_id, []).append((sender_name, text))
            bucket = self._unread_messages.setdefault(peer_id, [])
            bucket.append((sender_name, text))
            return len(bucket)

    def pop_unread(self, peer_id: str) -> list[tuple[str, str]]:
        """Drain and return buffered unread messages for peer_id."""
        with self._lock:
            return self._unread_messages.pop(peer_id, [])

    def record_message(self, peer_id: str, sender_name: str, text: str) -> None:
        """Append a displayed or sent message to thread-safe chat history."""
        with self._lock:
            self._chat_histories.setdefault(peer_id, []).append((sender_name, text))

    def get_unread_count(self, peer_id: str) -> int:
        """Return unread count for peer_id."""
        with self._lock:
            return len(self._unread_messages.get(peer_id, []))

    def pop_all_conns(self) -> list[PeerConnection]:
        """Remove and return all active connections for clean shutdown."""
        with self._lock:
            conns = list(self._active_conns.values())
            self._active_conns.clear()
            self._current_partner_id = None
            return conns
