"""UDP Peer Discovery & online status module (Owner: Thanh vien 2).

Implements Section 3.1, 3.2, and 5.1 (UDP broadcaster + UDP listener threads):
- Broadcast HELLO|<name>|<tcp_port>|<status> every 3 seconds on UDP port 9999.
- Listen on UDP port 9999 (reuseaddr/reuseport/broadcast), ignore self packets.
- Remove peers silent for > 10 seconds and notify online/offline transitions.
"""

from __future__ import annotations

from typing import Any, Optional

from protocol import DEFAULT_UDP_PORT, PEER_TIMEOUT_SEC, PRESENCE_INTERVAL_SEC
from state import Peer, SharedState


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

    def start(self) -> None:
        """Start UDP broadcaster and UDP listener daemon threads."""
        raise NotImplementedError("TV2: Implement UDP broadcaster & listener startup.")

    def stop(self) -> None:
        """Stop UDP discovery threads and close UDP sockets."""
        raise NotImplementedError("TV2: Implement UDP discovery shutdown.")

    def broadcast_now(self) -> None:
        """Immediately send one HELLO presence packet (e.g. after /status)."""
        raise NotImplementedError("TV2: Implement immediate UDP HELLO broadcast.")

    def format_online_list(self) -> list[str]:
        """Return formatted /list lines, e.g. ['1. Minh (192.168.1.12) online']."""
        raise NotImplementedError("TV2: Implement formatted /list output via state.")

    def resolve_peer(self, target: str) -> Optional[Peer]:
        """Resolve /chat <name|number> target into an online Peer snapshot."""
        raise NotImplementedError("TV2: Implement peer lookup via state.resolve_peer.")

    def set_status(self, status: str) -> bool:
        """Validate and set status ('online' or 'away'), then trigger broadcast_now()."""
        raise NotImplementedError("TV2: Implement /status <online|away> validation.")
