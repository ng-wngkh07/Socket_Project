"""UDP Peer Discovery & online status module (Owner: Thanh vien 2).

Implements Section 3.1, 3.2, and 5.1 (UDP broadcaster + UDP listener threads):
- Broadcast HELLO|<name>|<tcp_port>|<status> every 3 seconds on UDP port 9999.
- Listen on UDP port 9999 (reuseaddr/reuseport/broadcast), ignore self packets.
- Remove peers silent for > 10 seconds and notify online/offline transitions.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Any, Optional

from protocol import (
    DEFAULT_UDP_PORT,
    PEER_TIMEOUT_SEC,
    PRESENCE_INTERVAL_SEC,
    encode_hello,
    parse_hello,
)
from state import VALID_STATUSES, Peer, SharedState


class PeerDiscovery:
    """Manages UDP broadcast presence and online-peer table maintenance."""

    def __init__(
        self,
        state: SharedState,
        ui: Any,
        udp_port: int = DEFAULT_UDP_PORT,
        bcast_addr: str = "255.255.255.255",
        broadcast_interval: float = PRESENCE_INTERVAL_SEC,
        peer_timeout: float = PEER_TIMEOUT_SEC,
    ) -> None:
        self.state = state
        self.ui = ui
        self.udp_port = udp_port
        self.bcast_addr = bcast_addr
        self.broadcast_interval = broadcast_interval
        self.peer_timeout = peer_timeout

        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._send_sock: Optional[socket.socket] = None
        self._recv_sock: Optional[socket.socket] = None
        self._bcast_thread: Optional[threading.Thread] = None
        self._listen_thread: Optional[threading.Thread] = None

    @staticmethod
    def _configure_udp_socket(sock: socket.socket) -> None:
        """Apply required socket options (SO_REUSEADDR, SO_REUSEPORT, SO_BROADCAST)."""
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def _safe_notify(self, text: str) -> None:
        """Invoke ui.notify safely, falling back to print if ConsoleUI is still a stub."""
        if self.ui is None:
            return
        try:
            self.ui.notify(text)
        except NotImplementedError:
            print(text)

    def start(self) -> None:
        """Start UDP broadcaster and UDP listener daemon threads."""
        with self._state_lock:
            if (
                self._bcast_thread is not None
                and self._bcast_thread.is_alive()
                and self._listen_thread is not None
                and self._listen_thread.is_alive()
            ):
                return

            self._stop_event.clear()

            recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self._configure_udp_socket(recv_sock)
                recv_sock.bind(("", self.udp_port))
                recv_sock.settimeout(1.0)
            except Exception:
                recv_sock.close()
                raise

            send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                self._configure_udp_socket(send_sock)
            except Exception:
                recv_sock.close()
                send_sock.close()
                raise

            self._recv_sock = recv_sock
            self._send_sock = send_sock

            self._listen_thread = threading.Thread(
                target=self._listener_loop,
                name="UDP-Listener",
                daemon=True,
            )
            self._bcast_thread = threading.Thread(
                target=self._broadcaster_loop,
                name="UDP-Broadcaster",
                daemon=True,
            )
            self._listen_thread.start()
            self._bcast_thread.start()

    def stop(self) -> None:
        """Stop UDP discovery threads and close UDP sockets."""
        self._stop_event.set()

        with self._state_lock:
            send_sock = self._send_sock
            recv_sock = self._recv_sock
            bcast_thread = self._bcast_thread
            listen_thread = self._listen_thread
            self._send_sock = None
            self._recv_sock = None
            self._bcast_thread = None
            self._listen_thread = None

        for sock in (send_sock, recv_sock):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

        current = threading.current_thread()
        for thread in (bcast_thread, listen_thread):
            if thread is not None and thread.is_alive() and thread is not current:
                thread.join(timeout=2.0)

    def broadcast_now(self) -> None:
        """Immediately send one HELLO presence packet (e.g. on startup or after /status)."""
        packet = encode_hello(
            self.state.my_name,
            self.state.my_tcp_port,
            self.state.get_my_status(),
        )
        with self._state_lock:
            sock = self._send_sock

        if sock is not None:
            try:
                sock.sendto(packet, (self.bcast_addr, self.udp_port))
            except OSError:
                pass
            return

        # Fallback one-shot UDP socket if broadcast_now() is called before start()
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self._configure_udp_socket(temp_sock)
            temp_sock.sendto(packet, (self.bcast_addr, self.udp_port))
        except OSError:
            pass
        finally:
            try:
                temp_sock.close()
            except OSError:
                pass

    def _broadcaster_loop(self) -> None:
        """Periodically broadcast HELLO packet every broadcast_interval seconds."""
        while not self._stop_event.is_set():
            self.broadcast_now()
            if self._stop_event.wait(self.broadcast_interval):
                break

    def _process_datagram(
        self,
        data: bytes,
        sender_ip: str,
        now: Optional[float] = None,
    ) -> Optional[tuple[Peer, Optional[str]]]:
        """Parse and validate an incoming UDP datagram, updating SharedState if valid."""
        parsed = parse_hello(data)
        if parsed is None:
            return None

        name, tcp_port, status = parsed
        # Ignore our own broadcast (compare both name and tcp_port for single-PC & LAN)
        if name == self.state.my_name and tcp_port == self.state.my_tcp_port:
            return None

        peer, event = self.state.upsert_peer(
            name=name,
            ip=sender_ip,
            tcp_port=tcp_port,
            status=status,
            now=now,
        )
        if event == "online":
            self._safe_notify(f"[*] {peer.name} ({peer.ip}) is online")
        return peer, event

    def _prune_and_notify(self, now: Optional[float] = None) -> list[Peer]:
        """Remove peers silent longer than peer_timeout and notify the console."""
        expired = self.state.prune_expired(timeout_sec=self.peer_timeout, now=now)
        for peer in expired:
            self._safe_notify(f"[*] {peer.name} went offline")
        return expired

    def _listener_loop(self) -> None:
        """Listen for UDP HELLO datagrams and prune silent peers on a 1.0s wakeup cycle."""
        while not self._stop_event.is_set():
            with self._state_lock:
                sock = self._recv_sock
            if sock is None:
                break

            try:
                data, addr = sock.recvfrom(4096)
                sender_ip = addr[0]
                self._process_datagram(data, sender_ip, now=time.monotonic())
            except socket.timeout:
                pass
            except OSError:
                if self._stop_event.is_set():
                    break
            finally:
                if not self._stop_event.is_set():
                    self._prune_and_notify(now=time.monotonic())

    def format_online_list(self) -> list[str]:
        """Return formatted /list lines, e.g. ['  1. Minh     (192.168.1.12)  online']."""
        peers = self.state.get_online_peers_snapshot()
        return [
            f"  {idx}. {peer.name:<8} ({peer.ip})  {peer.status}"
            for idx, peer in enumerate(peers, start=1)
        ]

    def resolve_peer(self, target: str) -> Optional[Peer]:
        """Resolve /chat <name|number> target into an online Peer snapshot."""
        return self.state.resolve_peer(target)

    def set_status(self, status: str) -> bool:
        """Validate and set status ('online' or 'away'), then trigger broadcast_now()."""
        if not isinstance(status, str):
            self._safe_notify("[!] Invalid status. Usage: /status <online|away>")
            return False

        normalized = status.strip().lower()
        if normalized not in VALID_STATUSES:
            self._safe_notify("[!] Invalid status. Usage: /status <online|away>")
            return False

        self.state.set_my_status(normalized)
        self.broadcast_now()
        return True
