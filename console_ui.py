"""Console CLI & non-breaking input redraw UI (Owner: Thanh vien 3).

Implements Section 4.1, 4.2, 4.3, and 5.3:
- Main thread parses: /list, /chat <name|number>, <any text>, /send <file>,
  /leave, /status <online|away>, /help, /quit.
- Thread-safe notify() & display_chat() using ANSI erase/redraw protected by
  a screen lock so incoming messages never break what the user is typing.
"""

from __future__ import annotations

import os
import sys
import threading
from typing import Callable, Optional, TextIO

PROMPT = "> "
ANSI_CLEAR_LINE = "\r\033[K"

HELP_TEXT = "\n".join(
    [
        "Available commands:",
        "  /list                  Show online users",
        "  /chat <name|number>    Open or switch chat with a peer",
        "  <any text>             Send a message to the current chat partner",
        "  /send <file>           Send a file to the current chat partner",
        "  /leave                 Close the current chat",
        "  /status <online|away>  Change your online status",
        "  /help                  Show all commands",
        "  /quit                  Exit the program",
    ]
)


class ConsoleUI:
    """Thread-safe console interface protecting active input while printing events."""

    def __init__(
        self,
        stdin: Optional[TextIO] = None,
        stdout: Optional[TextIO] = None,
    ) -> None:
        self._stdin = stdin
        self._stdout = stdout
        self._screen_lock = threading.Lock()
        self._input_buffer: list[str] = []
        self._prompt_active = False
        self._running = False
        self._ansi_ready = False

    def _in(self) -> TextIO:
        return sys.stdin if self._stdin is None else self._stdin

    def _out(self) -> TextIO:
        return sys.stdout if self._stdout is None else self._stdout

    def _is_interactive(self) -> bool:
        try:
            return bool(self._in().isatty() and self._out().isatty())
        except OSError:
            return False

    def _enable_ansi_if_needed(self) -> None:
        if self._ansi_ready or self._stdout is not None:
            return
        if sys.platform == "win32":
            # Enables Virtual Terminal Processing (\033[K) on Windows console hosts.
            os.system("")
        out = self._out()
        if hasattr(out, "reconfigure"):
            try:
                out.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, OSError, ValueError):
                pass
        self._ansi_ready = True

    def _write_line(self, text: str) -> None:
        with self._screen_lock:
            out = self._out()
            if self._prompt_active:
                typed = "".join(self._input_buffer)
                out.write(f"{ANSI_CLEAR_LINE}{text}\n{PROMPT}{typed}")
            else:
                out.write(f"{text}\n")
            out.flush()

    def notify(self, text: str) -> None:
        """Safely print a system/status/unread notification without breaking input."""
        self._write_line(text)

    def display_chat(self, sender: str, text: str) -> None:
        """Safely print '[<sender>] <text>' without breaking active typed input."""
        self._write_line(f"[{sender}] {text}")

    def _start_prompt(self) -> None:
        with self._screen_lock:
            self._input_buffer.clear()
            self._prompt_active = True
            out = self._out()
            out.write(PROMPT)
            out.flush()

    def _handle_char(self, ch: str) -> None:
        with self._screen_lock:
            self._input_buffer.append(ch)
            out = self._out()
            out.write(ch)
            out.flush()

    def _handle_backspace(self) -> None:
        with self._screen_lock:
            if not self._input_buffer:
                return
            self._input_buffer.pop()
            out = self._out()
            out.write(f"{ANSI_CLEAR_LINE}{PROMPT}{''.join(self._input_buffer)}")
            out.flush()

    def _finish_entered_line(self) -> str:
        with self._screen_lock:
            line = "".join(self._input_buffer)
            self._input_buffer.clear()
            self._prompt_active = False
            stripped = line.strip()
            out = self._out()
            # Erase raw typed text for chat lines so display_chat prints '[Name] text' cleanly.
            if stripped and not stripped.startswith("/"):
                out.write(ANSI_CLEAR_LINE)
            else:
                out.write("\n")
            out.flush()
            return line

    def _abort_prompt(self) -> None:
        with self._screen_lock:
            self._input_buffer.clear()
            self._prompt_active = False
            out = self._out()
            out.write("\n")
            out.flush()

    def _read_line_windows(self) -> Optional[str]:
        import msvcrt

        while self._running:
            ch = msvcrt.getwch()
            if ch in ("\x00", "\xe0"):
                msvcrt.getwch()
                continue
            if ch == "\x03":
                self._abort_prompt()
                return None
            if ch in ("\x04", "\x1a"):
                with self._screen_lock:
                    empty = not self._input_buffer
                if empty:
                    self._abort_prompt()
                    return None
                continue
            if ch in ("\r", "\n"):
                return self._finish_entered_line()
            if ch in ("\x08", "\x7f"):
                self._handle_backspace()
                continue
            if ch.isprintable():
                self._handle_char(ch)
        return None

    def _read_line_posix(self) -> Optional[str]:
        import select
        import termios
        import tty

        inp = self._in()
        fd = inp.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while self._running:
                ch = inp.read(1)
                if ch == "":
                    self._abort_prompt()
                    return None
                if ch == "\x04":
                    with self._screen_lock:
                        empty = not self._input_buffer
                    if empty:
                        self._abort_prompt()
                        return None
                    continue
                if ch == "\x1b":
                    # Drain CSI escape sequences (arrow/home/end keys) so they do not pollute input.
                    while select.select([inp], [], [], 0.01)[0]:
                        nxt = inp.read(1)
                        if not nxt or nxt.isalpha() or nxt == "~":
                            break
                    continue
                if ch in ("\r", "\n"):
                    return self._finish_entered_line()
                if ch in ("\x7f", "\x08"):
                    self._handle_backspace()
                    continue
                if ch.isprintable():
                    self._handle_char(ch)
            return None
        except KeyboardInterrupt:
            self._abort_prompt()
            return None
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    def _read_line_interactive(self) -> Optional[str]:
        self._enable_ansi_if_needed()
        self._start_prompt()
        if sys.platform == "win32":
            return self._read_line_windows()
        return self._read_line_posix()

    def _read_line_stream(self) -> Optional[str]:
        raw = self._in().readline()
        if raw == "":
            return None
        return raw.rstrip("\r\n")

    def dispatch_line(self, raw_line: str, handlers: dict[str, Callable]) -> None:
        """Parse one line of console input and invoke the corresponding handler."""
        line = raw_line.strip()
        if not line:
            return

        if not line.startswith("/"):
            handlers["send_text"](line)
            return

        parts = line.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if cmd == "/list":
            if arg:
                self.notify("[!] Usage: /list")
                return
            entries = handlers["list"]()
            if entries:
                self.notify("\n".join(entries))
            else:
                self.notify("[*] No other peers online.")
        elif cmd == "/chat":
            if not arg:
                self.notify("[!] Usage: /chat <name|number>")
                return
            peer = handlers["resolve_peer"](arg)
            if peer is None:
                self.notify(
                    f"[!] Peer '{arg}' is offline or not found. Use /list to see online users."
                )
                return
            handlers["chat"](peer)
        elif cmd == "/send":
            if not arg:
                self.notify("[!] Usage: /send <file>")
                return
            partner = handlers["get_partner"]()
            if partner is None:
                self.notify("[!] No active chat partner. Use /chat <name|number> first.")
                return
            handlers["send_file"](partner, arg)
        elif cmd == "/leave":
            if arg:
                self.notify("[!] Usage: /leave")
                return
            handlers["leave"]()
        elif cmd == "/status":
            if not arg:
                self.notify("[!] Usage: /status <online|away>")
                return
            if handlers["status"](arg):
                self.notify(f"[*] Status changed to {arg.lower()}")
        elif cmd == "/help":
            self.notify(HELP_TEXT)
        elif cmd == "/quit":
            if arg:
                self.notify("[!] Usage: /quit")
                return
            self._running = False
        else:
            self.notify(f"[!] Unknown command '{cmd}'. Type /help for available commands.")

    def run_loop(self, handlers: dict[str, Callable]) -> None:
        """Run Main input loop dispatching the 8 Section 4.1 CLI commands."""
        self._running = True
        interactive = self._is_interactive()
        try:
            while self._running:
                line = (
                    self._read_line_interactive()
                    if interactive
                    else self._read_line_stream()
                )
                if line is None:
                    break
                self.dispatch_line(line, handlers)
        except KeyboardInterrupt:
            pass
        finally:
            with self._screen_lock:
                self._prompt_active = False
                self._input_buffer.clear()
            self._running = False
