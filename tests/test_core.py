"""Core unit tests for state.py, protocol.py, CLI parsing, and skeleton imports."""

from __future__ import annotations

import socket
import sys
import threading
from pathlib import Path
import unittest

SRC_DIR = Path(__file__).resolve().parent.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import console_ui  # noqa: E402
import discovery  # noqa: E402
import file_transfer  # noqa: E402
import lanchat  # noqa: E402
import protocol  # noqa: E402
import state  # noqa: E402
import tcp_chat  # noqa: E402


class TestProtocol(unittest.TestCase):
    def test_hello_roundtrip_and_validation(self) -> None:
        raw = protocol.encode_hello("Quan|A\n", 5000, "online")
        self.assertEqual(raw, b"HELLO|Quan_A|5000|online")
        self.assertEqual(protocol.parse_hello(raw), ("Quan_A", 5000, "online"))
        self.assertEqual(
            protocol.parse_hello(b"HELLO|Minh|6000|away"),
            ("Minh", 6000, "away"),
        )
        self.assertIsNone(protocol.parse_hello(b"BAD|Minh|6000|online"))
        self.assertIsNone(protocol.parse_hello(b"HELLO|Minh|99999|online"))
        self.assertIsNone(protocol.parse_hello(b"HELLO|Minh|6000|busy"))
        self.assertIsNone(protocol.parse_hello(b"\xff\xfe"))

    def test_tcp_framing_sticky_partial_unicode_and_pipe(self) -> None:
        s1, s2 = socket.socketpair()
        try:
            text_with_pipe = "Xin chao | tieng Viet co dau \n xuong dong!"
            combined = (
                protocol.encode_peer("Quan", 5001)
                + protocol.encode_msg(text_with_pipe)
                + protocol.encode_bye()
            )

            # Simulate fragmented TCP segments by sending 3 bytes at a time
            def trickle_send() -> None:
                for i in range(0, len(combined), 3):
                    s1.sendall(combined[i : i + 3])
                s1.close()

            t = threading.Thread(target=trickle_send, daemon=True)
            t.start()

            tag1, fields1 = protocol.read_frame_header(s2)
            self.assertEqual((tag1, fields1), ("PEER", ("Quan", 5001)))

            tag2, (msg_len,) = protocol.read_frame_header(s2)
            self.assertEqual(tag2, "MSG")
            payload = protocol.recv_exact(s2, msg_len).decode("utf-8")
            self.assertEqual(payload, text_with_pipe)

            tag3, fields3 = protocol.read_frame_header(s2)
            self.assertEqual((tag3, fields3), ("BYE", ()))
            t.join(timeout=2.0)
        finally:
            for s in (s1, s2):
                try:
                    s.close()
                except OSError:
                    pass

    def test_recv_exact_raises_on_premature_eof(self) -> None:
        s1, s2 = socket.socketpair()
        try:
            s1.sendall(b"abc")
            s1.close()
            with self.assertRaises(ConnectionError):
                protocol.recv_exact(s2, 10)
        finally:
            for s in (s1, s2):
                try:
                    s.close()
                except OSError:
                    pass

    def test_file_header_sanitizes_path_and_streams_chunks(self) -> None:
        hdr = protocol.encode_file_header("../../etc/slides|v1.pdf", 10)
        self.assertEqual(hdr, b"FILE|slides_v1.pdf|10\n")

        s1, s2 = socket.socketpair()
        try:
            s1.sendall(hdr + b"0123456789")
            s1.close()
            tag, (fname, size) = protocol.read_frame_header(s2)
            self.assertEqual((tag, fname, size), ("FILE", "slides_v1.pdf", 10))
            received = b"".join(protocol.iter_recv_chunks(s2, size, chunk_size=4))
            self.assertEqual(received, b"0123456789")
        finally:
            for s in (s1, s2):
                try:
                    s.close()
                except OSError:
                    pass


class TestSharedState(unittest.TestCase):
    def test_peer_upsert_resolve_prune_and_unread(self) -> None:
        st = state.SharedState("Quan", 5000)

        p1, ev1 = st.upsert_peer("Minh", "192.168.1.12", 6000, "online", now=100.0)
        p2, ev2 = st.upsert_peer("Huy", "192.168.1.15", 6001, "online", now=100.0)
        self.assertEqual(ev1, "online")
        self.assertEqual(ev2, "online")

        # Refresh Minh with status change
        _, ev1_status = st.upsert_peer("Minh", "192.168.1.12", 6000, "away", now=103.0)
        self.assertEqual(ev1_status, "status_changed")

        # Sorted order: Huy (index 1), Minh (index 2)
        snap = st.get_online_peers_snapshot()
        self.assertEqual([p.name for p in snap], ["Huy", "Minh"])
        self.assertEqual(st.resolve_peer("1").peer_id, p2.peer_id)
        self.assertEqual(st.resolve_peer("Minh").peer_id, p1.peer_id)
        self.assertIsNone(st.resolve_peer("99"))

        # Unread push & pop
        self.assertEqual(st.push_unread(p2.peer_id, "Huy", "msg1"), 1)
        self.assertEqual(st.push_unread(p2.peer_id, "Huy", "msg2"), 2)
        self.assertEqual(st.pop_unread(p2.peer_id), [("Huy", "msg1"), ("Huy", "msg2")])
        self.assertEqual(st.get_unread_count(p2.peer_id), 0)

        # Timeout pruning after 10s
        expired = st.prune_expired(timeout_sec=10.0, now=111.0)
        self.assertEqual([p.name for p in expired], ["Huy"])
        self.assertEqual([p.name for p in st.get_online_peers_snapshot()], ["Minh"])

    def test_remove_conn_checks_expected_sock(self) -> None:
        st = state.SharedState("Quan", 5000)
        peer = state.Peer("Minh", "127.0.0.1", 5002)
        s_old, s_dummy = socket.socketpair()
        s_new, s_dummy2 = socket.socketpair()
        try:
            st.set_current_partner(peer)
            st.register_conn(peer, s_old)
            _, prev = st.register_conn(peer, s_new)
            self.assertIs(prev, s_old)

            # Old receiver thread cleaning up must NOT evict newer socket
            removed, was_partner = st.remove_conn(peer.peer_id, expected_sock=s_old)
            self.assertIsNone(removed)
            self.assertFalse(was_partner)
            self.assertIs(st.get_conn(peer.peer_id).sock, s_new)
        finally:
            for s in (s_old, s_dummy, s_new, s_dummy2):
                s.close()


class TestEntryAndSkeletons(unittest.TestCase):
    def test_cli_arg_parser(self) -> None:
        args = lanchat.build_arg_parser().parse_args(
            ["--name", "Quan", "--port", "5001", "--bcast", "127.255.255.255"]
        )
        self.assertEqual(args.name, "Quan")
        self.assertEqual(args.port, 5001)
        self.assertEqual(args.udp_port, 9999)
        self.assertEqual(args.bcast, "127.255.255.255")

    def test_skeleton_classes_instantiate(self) -> None:
        st = state.SharedState("Quan", 5000)
        ui = console_ui.ConsoleUI()
        ft = file_transfer.FileTransferManager(st, ui)
        disc = discovery.PeerDiscovery(st, ui)
        chat = tcp_chat.TcpChatManager(st, ui, ft)
        self.assertIsNotNone(disc)
        self.assertIsNotNone(chat)


if __name__ == "__main__":
    unittest.main()
