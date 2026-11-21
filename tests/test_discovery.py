"""Unit tests for UDP PeerDiscovery module (Owner: Thanh vien 2)."""

from __future__ import annotations

import socket
import sys
import time
from pathlib import Path
import unittest

SRC_DIR = Path(__file__).resolve().parent.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from discovery import PeerDiscovery  # noqa: E402
from protocol import encode_hello  # noqa: E402
from state import SharedState  # noqa: E402


class _MockUI:
    """Thread-safe mock ConsoleUI recording notify() messages."""

    def __init__(self) -> None:
        self.notifications: list[str] = []

    def notify(self, text: str) -> None:
        self.notifications.append(text)


def _find_free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
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


if __name__ == "__main__":
    unittest.main()
