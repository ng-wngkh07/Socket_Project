from __future__ import annotations

import socket
import threading
from typing import Any, Optional

from protocol import (
    DEFAULT_TCP_PORT,
    ProtocolError,
    encode_bye,
    encode_msg,
    encode_peer,
    read_frame_header,
    recv_exact,
    send_frame,
)
from state import Peer, SharedState


class TcpChatManager:
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
        self.bound_port = tcp_port
        self._stop_event = threading.Event()
        self._listen_sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._sockets_lock = threading.Lock()
        self._open_sockets: set[socket.socket] = set()

    def _track_socket(self, sock: socket.socket) -> None:
        with self._sockets_lock:
            self._open_sockets.add(sock)

    def _close_quietly(self, sock: Optional[socket.socket]) -> None:
        if sock is None:
            return
        try:
            sock.close()
        except OSError:
            pass

    def _untrack_and_close(self, sock: Optional[socket.socket]) -> None:
        if sock is None:
            return
        with self._sockets_lock:
            self._open_sockets.discard(sock)
        self._close_quietly(sock)

    def start(self) -> None:
        if self._listen_sock is not None:
            return
        self._stop_event.clear()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        s.bind(("", self.tcp_port))
        s.listen(16)
        s.settimeout(0.5)
        self.bound_port = s.getsockname()[1]
        self._listen_sock = s
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        listen_sock = self._listen_sock
        self._listen_sock = None
        self._close_quietly(listen_sock)

        for conn in self.state.pop_all_conns():
            self._untrack_and_close(conn.sock)

        with self._sockets_lock:
            remaining = list(self._open_sockets)
            self._open_sockets.clear()
        for sock in remaining:
            self._close_quietly(sock)

        if self._accept_thread is not None and self._accept_thread.is_alive():
            self._accept_thread.join(timeout=1.5)
        self._accept_thread = None

    def connect_and_switch(self, peer: Optional[Peer]) -> None:
        if peer is None:
            self.ui.notify("[!] Peer is offline or not found.")
            return
        if peer.name == self.state.my_name and peer.tcp_port == self.state.my_tcp_port:
            self.ui.notify("[!] Cannot chat with yourself.")
            return

        conn = self.state.get_conn(peer.peer_id)
        if conn is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            try:
                sock.connect((peer.ip, peer.tcp_port))
                sock.settimeout(None)
                send_frame(sock, encode_peer(self.state.my_name, self.state.my_tcp_port))
            except OSError as exc:
                self._close_quietly(sock)
                self.ui.notify(
                    f"[!] Could not connect to {peer.name} ({peer.ip}:{peer.tcp_port}): {exc}"
                )
                return

            self._track_socket(sock)
            self.state.register_conn_if_absent(peer, sock)
            threading.Thread(
                target=self._receiver_loop,
                args=(sock, peer.ip, peer),
                daemon=True,
            ).start()

        self.state.set_current_partner(peer)
        unread = self.state.pop_unread(peer.peer_id)
        if unread:
            self.ui.notify(f"[*] Switched to {peer.name} \u2014 {len(unread)} unread:")
            for sender_name, text in unread:
                self.ui.display_chat(sender_name, text)
        else:
            self.ui.notify(f"[*] Connected to {peer.name} ({peer.ip}:{peer.tcp_port})")

    def send_chat(self, text: str) -> None:
        partner = self.state.get_current_partner()
        if partner is None:
            self.ui.notify("[!] No active chat partner. Use /list and /chat <name|number> first.")
            return

        conn = self.state.get_conn(partner.peer_id)
        if conn is None:
            self.state.set_current_partner(None)
            self.ui.notify(f"[*] Connection to {partner.name} is closed.")
            return

        try:
            send_frame(conn.sock, encode_msg(text), conn.send_lock)
        except OSError:
            self._handle_disconnect(partner, conn.sock, f"[*] {partner.name} disconnected.")
            return

        self.state.record_message(partner.peer_id, self.state.my_name, text)
        self.ui.display_chat(self.state.my_name, text)

    def leave_chat(self) -> None:
        partner = self.state.get_current_partner()
        if partner is None:
            self.ui.notify("[!] Not currently in a chat.")
            return

        removed, _ = self.state.remove_conn(partner.peer_id)
        if removed is not None:
            try:
                send_frame(removed.sock, encode_bye(), removed.send_lock)
            except OSError:
                pass
            self._untrack_and_close(removed.sock)
        else:
            self.state.set_current_partner(None)
        self.ui.notify(f"[*] Left chat with {partner.name}.")

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            listen_sock = self._listen_sock
            if listen_sock is None:
                break
            try:
                conn, addr = listen_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            self._track_socket(conn)
            threading.Thread(
                target=self._receiver_loop,
                args=(conn, addr[0], None),
                daemon=True,
            ).start()

    def _handle_disconnect(
        self,
        peer: Optional[Peer],
        sock: socket.socket,
        message: str,
    ) -> None:
        if peer is None:
            return
        removed, _ = self.state.remove_conn(peer.peer_id, expected_sock=sock)
        if removed is not None:
            self.ui.notify(message)

    def _handle_incoming_message(self, peer: Peer, text: str) -> None:
        if peer.peer_id == self.state.get_current_partner_id():
            self.state.record_message(peer.peer_id, peer.name, text)
            self.ui.display_chat(peer.name, text)
            return

        unread_count = self.state.push_unread(peer.peer_id, peer.name, text)
        self.ui.notify(f"[*] New message from {peer.name} ({unread_count} unread)")

    def _receiver_loop(
        self,
        sock: socket.socket,
        remote_ip: str,
        initial_peer: Optional[Peer],
    ) -> None:
        peer = initial_peer
        is_file_socket = False
        try:
            while not self._stop_event.is_set():
                try:
                    tag, fields = read_frame_header(sock)
                except (ConnectionError, ProtocolError, OSError):
                    if not self._stop_event.is_set() and not is_file_socket and peer is not None:
                        self._handle_disconnect(peer, sock, f"[*] {peer.name} disconnected.")
                    break

                if tag == "PEER":
                    peer_name, peer_tcp_port = fields
                    peer = Peer(name=peer_name, ip=remote_ip, tcp_port=peer_tcp_port)
                    self.state.register_conn_if_absent(peer, sock)
                elif tag == "MSG":
                    (msg_len,) = fields
                    try:
                        payload = recv_exact(sock, msg_len)
                    except (ConnectionError, OSError):
                        if not self._stop_event.is_set() and peer is not None:
                            self._handle_disconnect(peer, sock, f"[*] {peer.name} disconnected.")
                        break
                    text = payload.decode("utf-8", errors="replace")
                    if peer is None:
                        peer = Peer(name=remote_ip, ip=remote_ip, tcp_port=0)
                    self.state.register_conn_if_absent(peer, sock)
                    self._handle_incoming_message(peer, text)
                elif tag == "FILE":
                    filename, size_bytes = fields
                    is_file_socket = True
                    sender_name = remote_ip if peer is None else peer.name
                    if peer is not None:
                        self.state.remove_conn(
                            peer.peer_id,
                            expected_sock=sock,
                            clear_partner=False,
                        )
                    if self.file_handler is not None:
                        self.file_handler.receive_file_stream(
                            sock,
                            sender_name,
                            filename,
                            size_bytes,
                        )
                    break
                elif tag == "BYE":
                    if peer is not None:
                        self._handle_disconnect(peer, sock, f"[*] {peer.name} left the chat.")
                    break
        finally:
            self._untrack_and_close(sock)
