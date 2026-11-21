from __future__ import annotations

import socket
import sys
import threading
import time
from pathlib import Path
import unittest

SRC_DIR = Path(__file__).resolve().parent.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from protocol import encode_file_header, encode_peer, recv_exact
from state import Peer, SharedState
from tcp_chat import TcpChatManager


def _reserve_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeUI:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: list[tuple[str, str, str]] = []
        self._changed = threading.Condition(self._lock)

    def notify(self, text: str) -> None:
        with self._changed:
            self.events.append(("notify", "", text))
            self._changed.notify_all()

    def display_chat(self, sender: str, text: str) -> None:
        with self._changed:
            self.events.append(("chat", sender, text))
            self._changed.notify_all()

    def wait_for(self, predicate, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._changed:
            while not predicate(self.events):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._changed.wait(timeout=remaining)
        return True

    def notifications(self) -> list[str]:
        with self._lock:
            return [text for kind, _, text in self.events if kind == "notify"]

    def chats(self) -> list[tuple[str, str]]:
        with self._lock:
            return [(sender, text) for kind, sender, text in self.events if kind == "chat"]


class FakeFileHandler:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.received: list[tuple[str, str, bytes]] = []
        self._changed = threading.Condition(self._lock)

    def receive_file_stream(
        self,
        sock: socket.socket,
        sender_name: str,
        filename: str,
        size_bytes: int,
    ) -> None:
        data = recv_exact(sock, size_bytes)
        with self._changed:
            self.received.append((sender_name, filename, data))
            self._changed.notify_all()

    def wait_count(self, expected: int, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._changed:
            while len(self.received) < expected:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._changed.wait(timeout=remaining)
        return True


class TestTcpChatManager(unittest.TestCase):
    def setUp(self) -> None:
        self._managers: list[TcpChatManager] = []

    def tearDown(self) -> None:
        for mgr in reversed(self._managers):
            mgr.stop()

    def _make_peer_node(self, name: str) -> tuple[SharedState, FakeUI, FakeFileHandler, TcpChatManager, Peer]:
        port = _reserve_free_port()
        st = SharedState(name, port)
        ui = FakeUI()
        fh = FakeFileHandler()
        mgr = TcpChatManager(st, ui, fh, tcp_port=port)
        mgr.start()
        self._managers.append(mgr)
        peer = Peer(name=name, ip="127.0.0.1", tcp_port=mgr.bound_port)
        return st, ui, fh, mgr, peer

    def test_bidirectional_chat_and_leave_reconnect(self) -> None:
        st_q, ui_q, _, mgr_q, peer_q = self._make_peer_node("Quan")
        st_m, ui_m, _, mgr_m, peer_m = self._make_peer_node("Minh")

        mgr_q.connect_and_switch(peer_m)
        self.assertIn(
            f"[*] Connected to Minh (127.0.0.1:{peer_m.tcp_port})",
            ui_q.notifications(),
        )

        mgr_q.send_chat("Hi Minh")
        self.assertTrue(ui_m.wait_for(lambda ev: ("notify", "", "[*] New message from Quan (1 unread)") in ev))

        mgr_m.connect_and_switch(peer_q)
        self.assertTrue(
            ui_m.wait_for(
                lambda ev: ("notify", "", "[*] Switched to Quan \u2014 1 unread:") in ev
                and ("chat", "Quan", "Hi Minh") in ev
            )
        )

        mgr_m.send_chat("Yes!")
        self.assertTrue(ui_q.wait_for(lambda ev: ("chat", "Minh", "Yes!") in ev))

        mgr_q.leave_chat()
        self.assertIsNone(st_q.get_current_partner())
        self.assertTrue(ui_m.wait_for(lambda ev: any("Quan left the chat." in t for _, _, t in ev)))
        self.assertIsNone(st_m.get_current_partner())

        mgr_q.connect_and_switch(peer_m)
        mgr_q.send_chat("Back again")
        self.assertTrue(ui_m.wait_for(lambda ev: ("notify", "", "[*] New message from Quan (1 unread)") in ev))

    def test_unread_buffering_across_three_peers(self) -> None:
        _, _, _, mgr_q, peer_q = self._make_peer_node("Quan")
        _, ui_m, _, mgr_m, peer_m = self._make_peer_node("Minh")
        _, ui_h, _, mgr_h, peer_h = self._make_peer_node("Huy")

        mgr_m.connect_and_switch(peer_h)
        mgr_h.connect_and_switch(peer_m)
        mgr_q.connect_and_switch(peer_m)
        mgr_q.send_chat("Minh, did you get the slides?")
        mgr_q.send_chat("need to check before 3pm")

        self.assertTrue(
            ui_m.wait_for(
                lambda ev: ("notify", "", "[*] New message from Quan (1 unread)") in ev
                and ("notify", "", "[*] New message from Quan (2 unread)") in ev
            )
        )
        self.assertEqual(ui_m.chats(), [])

        mgr_h.send_chat("the router config is done")
        self.assertTrue(ui_m.wait_for(lambda ev: ("chat", "Huy", "the router config is done") in ev))

        mgr_m.connect_and_switch(peer_q)
        notes = ui_m.notifications()
        self.assertIn("[*] Switched to Quan \u2014 2 unread:", notes)
        self.assertIn(("Quan", "Minh, did you get the slides?"), ui_m.chats())
        self.assertIn(("Quan", "need to check before 3pm"), ui_m.chats())
        self.assertIn(("Huy", "the router config is done"), ui_m.chats())

    def test_error_handling_offline_and_abrupt_disconnect(self) -> None:
        st_q, ui_q, _, mgr_q, _ = self._make_peer_node("Quan")
        _, ui_m, _, mgr_m, peer_m = self._make_peer_node("Minh")

        mgr_q.send_chat("nobody")
        self.assertTrue(any("No active chat partner" in n for n in ui_q.notifications()))

        mgr_q.connect_and_switch(None)
        self.assertTrue(any("offline or not found" in n for n in ui_q.notifications()))

        closed_port = _reserve_free_port()
        mgr_q.connect_and_switch(Peer("Ghost", "127.0.0.1", closed_port))
        self.assertTrue(any("Could not connect to Ghost" in n for n in ui_q.notifications()))

        mgr_q.connect_and_switch(peer_m)
        mgr_m.stop()
        self.assertTrue(ui_q.wait_for(lambda ev: any("Minh disconnected." in t for _, _, t in ev)))
        self.assertIsNone(st_q.get_current_partner())

        mgr_q.send_chat("after crash")
        self.assertTrue(ui_q.notifications()[-1].startswith("[!] No active chat partner"))

    def test_dedicated_file_socket_does_not_disrupt_open_chat(self) -> None:
        _, ui_q, _, mgr_q, peer_q = self._make_peer_node("Quan")
        _, ui_m, fh_m, mgr_m, peer_m = self._make_peer_node("Minh")

        mgr_q.connect_and_switch(peer_m)
        mgr_m.connect_and_switch(peer_q)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as file_sock:
            file_sock.connect(("127.0.0.1", peer_m.tcp_port))
            payload = b"PDF_BINARY_BYTES"
            file_sock.sendall(
                encode_peer("Quan", peer_q.tcp_port)
                + encode_file_header("slides.pdf", len(payload))
                + payload
            )

        self.assertTrue(fh_m.wait_count(1))
        self.assertEqual(fh_m.received[0], ("Quan", "slides.pdf", b"PDF_BINARY_BYTES"))

        mgr_q.send_chat("sent slides")
        mgr_m.send_chat("got it")
        self.assertTrue(ui_m.wait_for(lambda ev: ("chat", "Quan", "sent slides") in ev))
        self.assertTrue(ui_q.wait_for(lambda ev: ("chat", "Minh", "got it") in ev))
        self.assertFalse(any("disconnected" in n for n in ui_m.notifications()))

    def test_simultaneous_connect_preserves_messaging(self) -> None:
        _, ui_q, _, mgr_q, peer_q = self._make_peer_node("Quan")
        _, ui_m, _, mgr_m, peer_m = self._make_peer_node("Minh")

        barrier = threading.Barrier(2)

        def connect_q() -> None:
            barrier.wait()
            mgr_q.connect_and_switch(peer_m)

        def connect_m() -> None:
            barrier.wait()
            mgr_m.connect_and_switch(peer_q)

        t1 = threading.Thread(target=connect_q)
        t2 = threading.Thread(target=connect_m)
        t1.start()
        t2.start()
        t1.join(timeout=3.0)
        t2.join(timeout=3.0)

        mgr_q.send_chat("hello from q")
        mgr_m.send_chat("hello from m")
        self.assertTrue(ui_m.wait_for(lambda ev: ("chat", "Quan", "hello from q") in ev))
        self.assertTrue(ui_q.wait_for(lambda ev: ("chat", "Minh", "hello from m") in ev))


if __name__ == "__main__":
    unittest.main()
