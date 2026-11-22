"""Unit & integration tests for ConsoleUI and FileTransferManager (Owner: Thanh vien 3)."""

from __future__ import annotations

import hashlib
import io
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from console_ui import ANSI_CLEAR_LINE, ConsoleUI, PROMPT  # noqa: E402
from file_transfer import FileTransferManager  # noqa: E402
from state import Peer, SharedState  # noqa: E402
from tcp_chat import TcpChatManager  # noqa: E402


def _reserve_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _RecordingUI:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self.events: list[tuple[str, str, str]] = []

    def notify(self, text: str) -> None:
        with self._cond:
            self.events.append(("notify", "", text))
            self._cond.notify_all()

    def display_chat(self, sender: str, text: str) -> None:
        with self._cond:
            self.events.append(("chat", sender, text))
            self._cond.notify_all()

    def wait_for(self, predicate, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._cond:
            while not predicate(self.events):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
        return True

    def notifications(self) -> list[str]:
        with self._lock:
            return [text for kind, _, text in self.events if kind == "notify"]


class TestConsoleUI(unittest.TestCase):
    def test_typing_while_messages_arrive_preserves_input_buffer(self) -> None:
        out = io.StringIO()
        ui = ConsoleUI(stdin=io.StringIO(), stdout=out)

        ui._start_prompt()
        for ch in "alo ban ":
            ui._handle_char(ch)

        # Simulate background thread delivering chat + system notification mid-typing.
        ui.display_chat("Minh", "ok, which part?")
        ui.notify("[*] New message from Huy (1 unread)")

        rendered = out.getvalue()
        self.assertIn(
            f"{ANSI_CLEAR_LINE}[Minh] ok, which part?\n{PROMPT}alo ban ",
            rendered,
        )
        self.assertIn(
            f"{ANSI_CLEAR_LINE}[*] New message from Huy (1 unread)\n{PROMPT}alo ban ",
            rendered,
        )

        # Backspace updates buffer and redraws cleanly; Enter commits full text.
        ui._handle_backspace()
        ui._handle_char("!")
        committed = ui._finish_entered_line()
        self.assertEqual(committed, "alo ban!")

    def test_concurrent_output_is_serialized_without_interleaving(self) -> None:
        out = io.StringIO()
        ui = ConsoleUI(stdin=io.StringIO(), stdout=out)
        threads = []
        for worker_id in range(6):
            def _worker(wid: int) -> None:
                for i in range(40):
                    ui.display_chat(f"User{wid}", f"line-{i}")

            t = threading.Thread(target=_worker, args=(worker_id,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 240)
        for line in lines:
            self.assertRegex(line, r"^\[User\d\] line-\d+$")

    def test_all_eight_commands_and_invalid_command_handling(self) -> None:
        peer_minh = Peer(name="Minh", ip="192.168.1.12", tcp_port=6000)
        partner_box: list[Peer | None] = [None]
        calls: list[tuple] = []

        def fake_list() -> list[str]:
            calls.append(("list",))
            return ["  1. Minh     (192.168.1.12)  online"]

        def fake_resolve(target: str) -> Peer | None:
            calls.append(("resolve", target))
            return peer_minh if target in ("1", "Minh") else None

        def fake_chat(peer: Peer) -> None:
            partner_box[0] = peer
            calls.append(("chat", peer.name))

        def fake_send_text(text: str) -> None:
            calls.append(("send_text", text))

        def fake_send_file(partner: Peer, path: str) -> None:
            calls.append(("send_file", partner.name, path))

        def fake_leave() -> None:
            partner_box[0] = None
            calls.append(("leave",))

        def fake_status(st: str) -> bool:
            calls.append(("status", st))
            return st in ("online", "away")

        script = "\n".join(
            [
                "/help",
                "/list",
                "/send slides.pdf",  # before selecting a partner
                "/chat Ghost",       # offline/unknown peer
                "/chat Minh",
                "Xin chao Minh!",
                "/send docs/my slides.pdf",
                "/status away",
                "/status invalid",
                "/unknowncmd",
                "/leave",
                "/quit",
                "ignored after quit",
            ]
        ) + "\n"

        out = io.StringIO()
        ui = ConsoleUI(stdin=io.StringIO(script), stdout=out)
        ui.run_loop(
            {
                "list": fake_list,
                "resolve_peer": fake_resolve,
                "status": fake_status,
                "chat": fake_chat,
                "send_text": fake_send_text,
                "leave": fake_leave,
                "send_file": fake_send_file,
                "get_partner": lambda: partner_box[0],
            }
        )

        output = out.getvalue()
        self.assertIn("Available commands:", output)
        self.assertIn("1. Minh", output)
        self.assertIn("No active chat partner", output)
        self.assertIn("Peer 'Ghost' is offline or not found", output)
        self.assertIn("Status changed to away", output)
        self.assertIn("Unknown command '/unknowncmd'", output)
        self.assertIn(("chat", "Minh"), calls)
        self.assertIn(("send_text", "Xin chao Minh!"), calls)
        self.assertIn(("send_file", "Minh", "docs/my slides.pdf"), calls)
        self.assertIn(("leave",), calls)
        self.assertNotIn(("send_text", "ignored after quit"), calls)


class TestFileTransferManager(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._managers: list[TcpChatManager] = []

    def tearDown(self) -> None:
        for mgr in reversed(self._managers):
            mgr.stop()
        self._tmp.cleanup()

    def _make_node(self, name: str) -> tuple[SharedState, _RecordingUI, FileTransferManager, TcpChatManager, Peer]:
        port = _reserve_free_port()
        st = SharedState(name, port)
        ui = _RecordingUI()
        dl_dir = os.path.join(self._tmp.name, f"dl_{name}")
        ft = FileTransferManager(state=st, ui=ui, downloads_dir=dl_dir)
        mgr = TcpChatManager(state=st, ui=ui, file_handler=ft, tcp_port=port)
        mgr.start()
        self._managers.append(mgr)
        peer = Peer(name=name, ip="127.0.0.1", tcp_port=mgr.bound_port)
        return st, ui, ft, mgr, peer

    def test_send_and_receive_binary_file_while_chatting(self) -> None:
        _, ui_q, ft_q, mgr_q, peer_q = self._make_node("Quan")
        _, ui_m, ft_m, mgr_m, peer_m = self._make_node("Minh")

        mgr_q.connect_and_switch(peer_m)
        mgr_m.connect_and_switch(peer_q)

        # Create a 1.5 MB deterministic binary payload.
        payload = (os.urandom(1024) * 1536)
        src_path = os.path.join(self._tmp.name, "slides.pdf")
        with open(src_path, "wb") as f:
            f.write(payload)

        ft_q.send_file_async(peer_m, src_path)

        # Chat concurrently while file transfer thread is running.
        mgr_q.send_chat("Dang gui file ne")
        mgr_m.send_chat("Ok dang nhan")

        self.assertTrue(
            ui_q.wait_for(lambda ev: any("100% Done." in text for _, _, text in ev))
        )
        self.assertTrue(
            ui_m.wait_for(lambda ev: any("Received slides.pdf" in text for _, _, text in ev))
        )
        self.assertTrue(
            ui_m.wait_for(lambda ev: ("chat", "Quan", "Dang gui file ne") in ev)
        )
        self.assertTrue(
            ui_q.wait_for(lambda ev: ("chat", "Minh", "Ok dang nhan") in ev)
        )

        # Verify percentage milestones were reported and saved file matches byte-for-byte.
        sender_notes = ui_q.notifications()
        self.assertTrue(any("%" in n and "100% Done." not in n for n in sender_notes))

        saved_path = os.path.join(ft_m.downloads_dir, "slides.pdf")
        self.assertTrue(os.path.isfile(saved_path))
        with open(saved_path, "rb") as f:
            self.assertEqual(
                hashlib.sha256(f.read()).hexdigest(),
                hashlib.sha256(payload).hexdigest(),
            )

    def test_interrupted_transfer_deletes_partial_file(self) -> None:
        st = SharedState("Minh", 5002)
        ui = _RecordingUI()
        dl_dir = os.path.join(self._tmp.name, "dl_partial")
        ft = FileTransferManager(state=st, ui=ui, downloads_dir=dl_dir, chunk_size=16384)

        s_send, s_recv = socket.socketpair()
        total_size = 512 * 1024
        partial_bytes = b"A" * (200 * 1024)

        def abort_mid_stream() -> None:
            try:
                s_send.sendall(partial_bytes)
            finally:
                s_send.close()

        t = threading.Thread(target=abort_mid_stream, daemon=True)
        t.start()
        try:
            ft.receive_file_stream(s_recv, "Quan", "big_video.bin", total_size)
        finally:
            s_recv.close()
            t.join(timeout=3.0)

        self.assertTrue(any("File transfer failed" in n for n in ui.notifications()))
        self.assertEqual(os.listdir(dl_dir), [])

    def test_path_traversal_sanitized_and_collision_safe(self) -> None:
        st = SharedState("Minh", 5002)
        ui = _RecordingUI()
        dl_dir = os.path.join(self._tmp.name, "dl_safe")
        ft = FileTransferManager(state=st, ui=ui, downloads_dir=dl_dir)

        for content in (b"first-version", b"second-version"):
            s_send, s_recv = socket.socketpair()
            try:
                s_send.sendall(content)
                s_send.close()
                ft.receive_file_stream(s_recv, "Quan", "../../outside\\report.txt", len(content))
            finally:
                s_recv.close()

        files = sorted(os.listdir(dl_dir))
        self.assertEqual(files, ["report.txt", "report_1.txt"])
        self.assertFalse(os.path.exists(os.path.join(self._tmp.name, "report.txt")))
        with open(os.path.join(dl_dir, "report.txt"), "rb") as f1:
            self.assertEqual(f1.read(), b"first-version")
        with open(os.path.join(dl_dir, "report_1.txt"), "rb") as f2:
            self.assertEqual(f2.read(), b"second-version")

    def test_missing_file_no_partner_and_zero_byte_file(self) -> None:
        _, ui_q, ft_q, _, peer_q = self._make_node("Quan")
        _, ui_m, ft_m, _, peer_m = self._make_node("Minh")

        ft_q.send_file_async(None, "anything.txt")
        ft_q.send_file_async(peer_m, os.path.join(self._tmp.name, "missing.txt"))
        self.assertTrue(any("No active chat partner" in n for n in ui_q.notifications()))
        self.assertTrue(any("File not found" in n for n in ui_q.notifications()))

        empty_path = os.path.join(self._tmp.name, "empty.txt")
        with open(empty_path, "wb"):
            pass
        ft_q.send_file_async(peer_m, empty_path)
        self.assertTrue(
            ui_m.wait_for(lambda ev: any("Received empty.txt" in text for _, _, text in ev))
        )
        saved_empty = os.path.join(ft_m.downloads_dir, "empty.txt")
        self.assertTrue(os.path.isfile(saved_empty))
        self.assertEqual(os.path.getsize(saved_empty), 0)


if __name__ == "__main__":
    unittest.main()
