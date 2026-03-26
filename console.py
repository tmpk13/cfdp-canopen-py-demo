from __future__ import annotations

import cmd
import sys
from pathlib import Path

from cfdppy import CfdpState

from cfdp_node import LiveEntity

_USE_COLOR = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

if _USE_COLOR:
    _RESET  = "\033[0m"
    _BOLD   = "\033[1m"
    _CYAN   = "\033[1;36m"
    _GREEN  = "\033[1;32m"
    _YELLOW = "\033[1;33m"
    _RED    = "\033[1;31m"
    # Readline markers: \001 and \002 prevent prompt breaking after output
    _RL_START = "\001"
    _RL_END   = "\002"
else:
    _RESET = _BOLD = _CYAN = _GREEN = _YELLOW = _RED = ""
    _RL_START = _RL_END = ""


def _rl(code: str) -> str:
    """Wrap an ANSI code in readline ignore markers (for use in prompts only)."""
    if not code:
        return ""
    return f"{_RL_START}{code}{_RL_END}"


class NodeConsole(cmd.Cmd):

    def __init__(self, entity: LiveEntity):
        super().__init__()
        self.entity = entity
        self.prompt = f"{_rl(_CYAN)}Entity-{entity.entity_id}{_rl(_RESET)}> "
        self.intro = (
            f"\n{_BOLD}  CFDP-over-CANopen  |  Entity {entity.entity_id}{_RESET}\n"
            f"  Peers: {entity.peer_ids}\n"
            f"  Type {_CYAN}help{_RESET} or {_CYAN}?{_RESET} for commands.\n"
        )

    # Commands
    def do_put(self, args: str):
        """put <dest_entity_id> <source_file> [<dest_file>]"""
        parts = args.split()
        if len(parts) < 2:
            print("Usage: put <dest_entity_id> <source_file> [<dest_file>]")
            return
        try:
            dest_id = int(parts[0])
        except ValueError:
            print(f"{_RED}Error: dest_id must be an integer{_RESET}")
            return
        src = Path(parts[1])
        dst = Path(parts[2]) if len(parts) >= 3 else Path(f"/tmp/recv_{src.name}")
        if not src.exists():
            print(f"{_RED}Error: {src} does not exist{_RESET}")
            return
        if dest_id not in self.entity.peer_ids:
            print(f"{_RED}Error: entity {dest_id} is not a known peer{_RESET}")
            print(f"  Known peers: {self.entity.peer_ids}")
            return
        if self.entity.source.state != CfdpState.IDLE:
            print(f"{_YELLOW}Warning: source handler is busy — wait for the current transfer to finish{_RESET}")
            return
        print(f"{_GREEN}Sending {src} -> entity {dest_id}:{dst}{_RESET}")
        self.entity.put_file(dest_id, src, dst)

    def do_cat(self, args: str):
        """cat <path>"""
        if not args:
            print("Usage: cat <path>")
            return
        path = Path(args.strip())
        if not path.exists():
            print(f"{_RED}Error: {path} does not exist{_RESET}")
            return
        MAX_READ = 1024 * 1024  # 1 MiB cap
        size = path.stat().st_size
        if size > MAX_READ:
            print(f"  ... (file too large, showing last {MAX_READ} bytes) ...")
            with open(path, "rb") as f:
                f.seek(-MAX_READ, 2)
                data = f.read()
        else:
            data = path.read_bytes()
        lines = data.decode(errors="replace").splitlines()
        if len(lines) > 40:
            print(f"  ... (showing last 40 of {len(lines)} lines) ...")
            lines = lines[-40:]
        print("\n".join(lines))

    def do_ls(self, args: str):
        """ls [<path>] (default: /tmp)"""
        directory = Path(args.strip()) if args.strip() else Path("/tmp")
        try:
            files = sorted(directory.iterdir())
        except OSError as e:
            print(f"{_RED}{e}{_RESET}")
            return
        for f in files:
            try:
                is_file = f.is_file()
                is_dir = f.is_dir()
                size = f.stat().st_size if is_file else 0
            except OSError:
                print(f"  {f.name}  (inaccessible)")
                continue
            tag = "/" if is_dir else ""
            print(f"  {f.name}{tag}  ({size} bytes)" if is_file else f"  {f.name}{tag}")

    def do_status(self, args: str):
        """Show source/dest handler state"""
        src = self.entity.source.state
        dst = self.entity.dest.state
        src_col = _GREEN if src == CfdpState.IDLE else _YELLOW
        dst_col = _GREEN if dst == CfdpState.IDLE else _YELLOW
        print(f"  Source handler: {src_col}{src.name}{_RESET}")
        print(f"  Dest   handler: {dst_col}{dst.name}{_RESET}")

    def do_wait(self, args: str):
        """wait [<timeout_secs>] (default: 30)"""
        try:
            timeout = float(args.strip()) if args.strip() else 30.0
        except ValueError:
            print(f"{_RED}Error: timeout must be a number{_RESET}")
            return
        tid = self.entity.user.last_transaction_id
        if tid is None:
            print(f"{_YELLOW}No active transaction to wait on.{_RESET}")
            return
        print(f"Waiting for transfer to finish (timeout {timeout}s) ...")
        ok = self.entity.user.finished_event(tid).wait(timeout)
        if ok:
            print(f"{_GREEN}Transfer finished.{_RESET}")
        else:
            print(f"{_YELLOW}Timed out waiting.{_RESET}")

    def do_quit(self, args: str):
        """Exit and shut down"""
        print("Shutting down ...")
        return True

    def default(self, line: str):
        token = line.strip()
        if token in ("exit", "q", "EOF"):
            return self.do_quit("")
        print(f"{_RED}Unknown command: {token}{_RESET}")
        print(f"Type {_CYAN}help{_RESET} or {_CYAN}?{_RESET} for available commands.")
