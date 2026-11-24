"""Unit and cross-module integration tests for UDP PeerDiscovery (Owner: Thanh vien 2)."""

from __future__ import annotations

import io
import socket
import sys
import time
from pathlib import Path
import unittest

SRC_DIR = Path(__file__).resolve().parent.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from console_ui import ANSI_CLEAR_LINE, PROMPT, ConsoleUI  # noqa: E402
from discovery import PeerDiscovery  # noqa: E402
from file_transfer import FileTransferManager  # noqa: E402
from protocol import encode_hello  # noqa: E402
from state import SharedState  # noqa: E402
from tcp_chat import TcpChatManager  # noqa: E402


class _MockUI:
    """Thread-safe mock ConsoleUI recording notify() and display_chat() messages."""

    def __init__(self) -> None:
        self.notifications: list[str] = []
        self.chats: list[tuple[str, str]] = []

    def notify(self, text: str) -> None:
        self.notifications.append(text)

    def display_chat(self, sender: str, text: str) -> None:
        self.chats.append((sender, text))


def _find_free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class TestPeerDiscovery(unittest.TestCase):
    def test_process_datagram_ignores_self_and_handles_peers(self) -> None:
        st = SharedState("Quan", 5001)
        ui = _MockUI()
        disc = PeerDiscovery(st, ui)

        # 1. Self packet (same name and tcp_port) must be ignored
        self_pkt = encode_hello("Quan", 5001, "online")
        self.assertIsNone(disc._process_datagram(self_pkt, "127.0.0.1", now=10.0))
        self.assertEqual(disc.format_online_list(), [])
        self.assertEqual(ui.notifications, [])

        # 2. Malformed packet must be ignored
        self.assertIsNone(disc._process_datagram(b"INVALID|PACKET", "127.0.0.1", now=10.0))
        self.assertEqual(disc.format_online_list(), [])

        # 3. Valid new peer triggers online notification and appears in /list
        minh_pkt = encode_hello("Minh", 5002, "online")
        res = disc._process_datagram(minh_pkt, "192.168.1.12", now=10.0)
        self.assertIsNotNone(res)
        peer, event = res
        self.assertEqual(event, "online")
        self.assertEqual(peer.name, "Minh")
        self.assertEqual(ui.notifications, ["[*] Minh (192.168.1.12) is online"])
        self.assertEqual(
            disc.format_online_list(),
            ["  1. Minh     (192.168.1.12)  online"],
        )

        # 4. Resolve by index ('1') and by name ('Minh' / 'minh')
        self.assertEqual(disc.resolve_peer("1").peer_id, peer.peer_id)
        self.assertEqual(disc.resolve_peer("Minh").peer_id, peer.peer_id)
        self.assertEqual(disc.resolve_peer("minh").peer_id, peer.peer_id)
        self.assertIsNone(disc.resolve_peer("2"))
        self.assertIsNone(disc.resolve_peer("Unknown"))

        # 5. Subsequent heartbeat with away status updates status without duplicate online alert
        minh_away = encode_hello("Minh", 5002, "away")
        _, event2 = disc._process_datagram(minh_away, "192.168.1.12", now=13.0)
        self.assertEqual(event2, "status_changed")
        self.assertEqual(len(ui.notifications), 1)
        self.assertEqual(
            disc.format_online_list(),
            ["  1. Minh     (192.168.1.12)  away"],
        )

        # 6. Prune after 10s silence removes peer and notifies offline
        expired = disc._prune_and_notify(now=23.5)
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0].name, "Minh")
        self.assertEqual(disc.format_online_list(), [])
        self.assertIn("[*] Minh went offline", ui.notifications)

    def test_set_status_validation(self) -> None:
        st = SharedState("Quan", 5001)
        ui = _MockUI()
        disc = PeerDiscovery(st, ui, udp_port=_find_free_udp_port(), bcast_addr="127.0.0.1")

        self.assertTrue(disc.set_status("away"))
        self.assertEqual(st.get_my_status(), "away")
        self.assertTrue(disc.set_status("  ONLINE "))
        self.assertEqual(st.get_my_status(), "online")

        self.assertFalse(disc.set_status("busy"))
        self.assertEqual(st.get_my_status(), "online")
        self.assertTrue(any("Invalid status" in msg for msg in ui.notifications))

    def test_two_instances_udp_discovery_status_and_timeout(self) -> None:
        udp_port = _find_free_udp_port()

        st1 = SharedState("Quan", 5001)
        ui1 = _MockUI()
        d1 = PeerDiscovery(
            state=st1,
            ui=ui1,
            udp_port=udp_port,
            bcast_addr="127.255.255.255",
            broadcast_interval=0.15,
            peer_timeout=0.8,
        )

        st2 = SharedState("Minh", 5002)
        ui2 = _MockUI()
        d2 = PeerDiscovery(
            state=st2,
            ui=ui2,
            udp_port=udp_port,
            bcast_addr="127.255.255.255",
            broadcast_interval=0.15,
            peer_timeout=0.8,
        )

        try:
            d1.start()
            d2.start()

            # Wait up to 1.5s for both instances to discover each other
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                if d1.resolve_peer("Minh") is not None and d2.resolve_peer("Quan") is not None:
                    break
                time.sleep(0.05)

            self.assertIsNotNone(d1.resolve_peer("Minh"))
            self.assertIsNotNone(d2.resolve_peer("Quan"))
            self.assertEqual(len(d1.format_online_list()), 1)
            self.assertIn("Minh", d1.format_online_list()[0])
            self.assertIn("online", d1.format_online_list()[0])

            # Minh changes status to away -> Quan sees away immediately
            self.assertTrue(d2.set_status("away"))
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                peer = d1.resolve_peer("Minh")
                if peer is not None and peer.status == "away":
                    break
                time.sleep(0.05)
            self.assertEqual(d1.resolve_peer("Minh").status, "away")

            # Minh stops -> Quan prunes Minh after peer_timeout and notifies offline
            d2.stop()
            deadline = time.monotonic() + 2.5
            while time.monotonic() < deadline:
                if d1.resolve_peer("Minh") is None:
                    break
                time.sleep(0.1)

            self.assertIsNone(d1.resolve_peer("Minh"))
            self.assertEqual(d1.format_online_list(), [])
            self.assertTrue(any("Minh went offline" in msg for msg in ui1.notifications))
        finally:
            d1.stop()
            d2.stop()

    def test_m3_discovery_to_tcp_chat_integration_via_console_commands(self) -> None:
        """Milestone 3 TV2 integration: /chat <number|name> resolves from UDP table and opens TCP chat."""
        udp_port = _find_free_udp_port()
        tcp_port_1 = _find_free_tcp_port()
        tcp_port_2 = _find_free_tcp_port()

        st1 = SharedState("Quan", tcp_port_1)
        out1 = io.StringIO()
        ui1 = ConsoleUI(stdin=io.StringIO(""), stdout=out1)
        ft1 = FileTransferManager(st1, ui1)
        d1 = PeerDiscovery(
            st1,
            ui1,
            udp_port=udp_port,
            bcast_addr="127.255.255.255",
            broadcast_interval=0.15,
            peer_timeout=2.0,
        )
        c1 = TcpChatManager(st1, ui1, ft1, tcp_port=tcp_port_1)

        st2 = SharedState("Minh", tcp_port_2)
        out2 = io.StringIO()
        ui2 = ConsoleUI(stdin=io.StringIO(""), stdout=out2)
        ft2 = FileTransferManager(st2, ui2)
        d2 = PeerDiscovery(
            st2,
            ui2,
            udp_port=udp_port,
            bcast_addr="127.255.255.255",
            broadcast_interval=0.15,
            peer_timeout=2.0,
        )
        c2 = TcpChatManager(st2, ui2, ft2, tcp_port=tcp_port_2)

        handlers1 = {
            "list": d1.format_online_list,
            "resolve_peer": d1.resolve_peer,
            "status": d1.set_status,
            "chat": c1.connect_and_switch,
            "send_text": c1.send_chat,
            "leave": c1.leave_chat,
            "send_file": ft1.send_file_async,
            "get_partner": st1.get_current_partner,
        }

        try:
            c1.start()
            c2.start()
            d1.start()
            d2.start()

            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                if d1.resolve_peer("1") is not None:
                    break
                time.sleep(0.05)

            self.assertIsNotNone(d1.resolve_peer("1"))

            # Quan runs '/chat 1' (resolves Minh via UDP discovery index) and sends a chat message
            ui1.dispatch_line("/chat 1", handlers1)
            ui1.dispatch_line("Xin chao Minh tu bang UDP discovery!", handlers1)

            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                if st2.get_unread_count(f"Quan@127.0.0.1:{tcp_port_1}") == 1:
                    break
                time.sleep(0.05)

            self.assertIn(f"[*] Connected to Minh (127.0.0.1:{tcp_port_2})", out1.getvalue())
            self.assertIn("[*] New message from Quan (1 unread)", out2.getvalue())
        finally:
            c1.stop()
            c2.stop()
            d1.stop()
            d2.stop()

    def test_m4_udp_notify_does_not_break_active_console_typing(self) -> None:
        """Milestone 4 TV2 integration: UDP online/offline notifications redraw active typed line."""
        out = io.StringIO()
        ui = ConsoleUI(stdin=io.StringIO(""), stdout=out)
        st = SharedState("Quan", 5001)
        disc = PeerDiscovery(st, ui)

        # Simulate user currently typing "> alo ban " without pressing Enter yet
        ui._start_prompt()
        for ch in "alo ban ":
            ui._handle_char(ch)

        # UDP listener receives a new peer HELLO packet in the background
        disc._process_datagram(encode_hello("Minh", 5002, "online"), "192.168.1.12", now=10.0)

        rendered = out.getvalue()
        self.assertIn(
            f"{ANSI_CLEAR_LINE}[*] Minh (192.168.1.12) is online\n{PROMPT}alo ban ",
            rendered,
        )

        # User finishes typing "ơi" and hits Enter
        for ch in "oi":
            ui._handle_char(ch)
        final_line = ui._finish_entered_line()
        self.assertEqual(final_line, "alo ban oi")


if __name__ == "__main__":
    unittest.main()
