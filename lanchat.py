"""LAN Chat (Peer-to-Peer) CLI entry point (Owners: TV2 CLI flags + TV1 wiring)."""

from __future__ import annotations

import argparse
from typing import Optional

from console_ui import ConsoleUI
from discovery import PeerDiscovery
from file_transfer import FileTransferManager
from protocol import DEFAULT_TCP_PORT, DEFAULT_UDP_PORT
from state import SharedState
from tcp_chat import TcpChatManager


def _valid_port(raw: str) -> int:
    port = int(raw)
    if not (1 <= port <= 65535):
        raise argparse.ArgumentTypeError(f"Port must be between 1 and 65535, got {raw}")
    return port


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser per Section 4.2 and Section 9 hints."""
    parser = argparse.ArgumentParser(
        prog="lanchat.py",
        description="P2P LAN Chat over UDP discovery & TCP messaging/file transfer.",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Display name shown to LAN peers.",
    )
    parser.add_argument(
        "--port",
        type=_valid_port,
        default=DEFAULT_TCP_PORT,
        help=f"TCP listening port (default: {DEFAULT_TCP_PORT}).",
    )
    parser.add_argument(
        "--udp-port",
        type=_valid_port,
        default=DEFAULT_UDP_PORT,
        help=f"UDP discovery broadcast/listen port (default: {DEFAULT_UDP_PORT}).",
    )
    parser.add_argument(
        "--bcast",
        default="255.255.255.255",
        help="UDP broadcast address (use 127.255.255.255 for single-PC tests).",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """Initialize components, start background threads, and run Console UI."""
    args = build_arg_parser().parse_args(argv)

    state = SharedState(my_name=args.name, my_tcp_port=args.port)
    ui = ConsoleUI()
    file_transfer = FileTransferManager(state=state, ui=ui)
    discovery = PeerDiscovery(
        state=state,
        ui=ui,
        udp_port=args.udp_port,
        bcast_addr=args.bcast,
    )
    chat = TcpChatManager(
        state=state,
        ui=ui,
        file_handler=file_transfer,
        tcp_port=args.port,
    )

    print(f"[*] Listening on TCP port {args.port}")
    print(f"[*] Broadcasting presence on UDP port {args.udp_port}")

    try:
        discovery.start()
        chat.start()
        handlers = {
            "list": discovery.format_online_list,
            "resolve_peer": discovery.resolve_peer,
            "status": discovery.set_status,
            "chat": chat.connect_and_switch,
            "send_text": chat.send_chat,
            "leave": chat.leave_chat,
            "send_file": file_transfer.send_file_async,
            "get_partner": state.get_current_partner,
        }
        ui.run_loop(handlers)
    finally:
        chat.stop()
        discovery.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
