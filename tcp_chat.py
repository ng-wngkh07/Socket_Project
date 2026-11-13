"""TCP P2P acceptor, per-peer receivers, and chat session manager (Owner: TV1 - Nhom truong).

Implements Section 3.3, 4.2, 5.1, 5.2, and 5.3:
- TCP acceptor thread accepting incoming peer chat & file-transfer sockets.
- Per-peer TCP receiver thread reading exact framed headers/payloads.
- Unread message buffering for non-active partners and safe disconnect handling.
"""

from __future__ import annotations

from typing import Any

from protocol import DEFAULT_TCP_PORT
from state import Peer, SharedState


class TcpChatManager:
    """Manages P2P TCP listening socket, peer receivers, and active chat switching."""

    def __init__(
        self,
        state: SharedState,
        ui: Any,
        file_handler: Any,
        tcp_port: int = DEFAULT_TCP_PORT,
    ) -> None:
        self.state = state
        self.ui = ui
        self.file_handler = file_handler
        self.tcp_port = tcp_port

    def start(self) -> None:
        """Bind listening TCP socket and start TCP acceptor daemon thread."""
        raise NotImplementedError("TV1: Implement TCP acceptor startup.")

    def stop(self) -> None:
        """Close listening socket and all active peer TCP connections."""
        raise NotImplementedError("TV1: Implement TCP chat shutdown.")

    def connect_and_switch(self, peer: Peer) -> None:
        """Open/reuse TCP connection to peer, switch current_partner, flush unread."""
        raise NotImplementedError("TV1: Implement /chat connection & unread flush.")

    def send_chat(self, text: str) -> None:
        """Send one chat message frame to the current active chat partner."""
        raise NotImplementedError("TV1: Implement sending framed MSG to current partner.")

    def leave_chat(self) -> None:
        """Send BYE frame, close/detach current chat session, clear current_partner."""
        raise NotImplementedError("TV1: Implement /leave chat termination.")
