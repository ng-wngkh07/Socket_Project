"""Console CLI & non-breaking input redraw UI (Owner: Thanh vien 3).

Implements Section 4.1, 4.2, 4.3, and 5.3:
- Main thread parses: /list, /chat <name|number>, <any text>, /send <file>,
  /leave, /status <online|away>, /help, /quit.
- Thread-safe notify() & display_chat() that do not break what user is typing.
"""

from __future__ import annotations

from typing import Callable


class ConsoleUI:
    """Thread-safe console interface protecting active input while printing events."""

    def __init__(self) -> None:
        """Initialize console lock and line-redraw / prompt_toolkit session."""
        self._running = False

    def notify(self, text: str) -> None:
        """Safely print a system/status/unread notification without breaking input."""
        raise NotImplementedError("TV3: Implement thread-safe console notification print.")

    def display_chat(self, sender: str, text: str) -> None:
        """Safely print '[<sender>] <text>' without breaking active typed input."""
        raise NotImplementedError("TV3: Implement thread-safe '[Name] text' print.")

    def run_loop(self, handlers: dict[str, Callable]) -> None:
        """Run Main input loop dispatching the 8 Section 4.1 CLI commands."""
        raise NotImplementedError("TV3: Implement Main console command & chat loop.")
