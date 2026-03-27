"""Auto-restarting process supervisor with multi-process management.

Manages multiple supervised processes, each in its own terminal window.
Provides pm2-style process listing with stats.

Usage:
    macf_tools auto-restart launch -- claude -c
    macf_tools auto-restart launch --name manny -- ssh pa_manny@localhost
    macf_tools auto-restart list                 # ps-style listing
    macf_tools auto-restart restart <pid>        # trigger restart
    macf_tools auto-restart disable <pid>        # stop auto-restart
    macf_tools auto-restart status <pid>         # detailed status

Architecture:
    launch → opens new terminal → runs supervisor loop in that terminal
    supervisor loop → spawns command as child, restarts on exit
    registry → /tmp/macf/auto-restart/*.json (one per supervised process)
    signals → SIGUSR1 (restart child), SIGUSR2 (disable loop)
"""

import json
import os
import platform
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REGISTRY_DIR = Path("/tmp/macf/auto-restart")


def _registry_file(pid: int) -> Path:
    return REGISTRY_DIR / f"{pid}.json"


def _write_registry(pid: int, data: dict):
    """Write process stats to registry."""
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    _registry_file(pid).write_text(json.dumps(data, indent=2))


def _read_registry(pid: int) -> dict:
    f = _registry_file(pid)
    if not f.exists():
        return {}
    return json.loads(f.read_text())


def _update_registry(pid: int, **kwargs):
    data = _read_registry(pid)
    data.update(kwargs)
    _write_registry(pid, data)


def _cleanup_registry(pid: int):
    f = _registry_file(pid)
    if f.exists():
        f.unlink()


def _notify_telegram(message: str, prefix: str = ""):
    try:
        from macf.channels.telegram import send_telegram_notification
        send_telegram_notification(message, prefix=prefix)
    except Exception:
        pass


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    remaining = minutes % 60
    if hours < 24:
        return f"{hours}h{remaining}m"
    days = hours // 24
    remaining_h = hours % 24
    return f"{days}d{remaining_h}h"


def launch(cmd_args: list, name: str = "",
           restart_delay: int = 5,
           new_window: bool = False,
           terminal: str = "auto") -> int:
    """Launch a supervised process.

    Default: runs supervisor loop directly in the current terminal.
    With --new-window: opens a new terminal window for the supervisor.

    Args:
        cmd_args: Command and arguments to supervise
        name: Optional display name (defaults to command basename)
        restart_delay: Seconds between restarts
        terminal: Terminal app to use: "auto", "terminal", "iterm2",
            "gnome-terminal", "xterm", "konsole"

    Returns:
        Supervisor PID
    """
    if not cmd_args:
        print("Error: no command specified", file=sys.stderr)
        return 1

    if not name:
        name = os.path.basename(cmd_args[0])

    # Direct mode (default): run supervisor loop in current terminal
    if not new_window:
        print(f"[auto-restart] Starting '{name}' (direct mode)")
        print(f"[auto-restart] Command: {' '.join(cmd_args)}")
        print(f"[auto-restart] Restart delay: {restart_delay}s")
        print(f"[auto-restart] Ctrl-C during countdown to stop\n")
        run_loop(cmd_args, name=name, restart_delay=restart_delay)
        return 0

    # New-window mode: build supervisor command for new terminal
    supervisor_cmd = [
        sys.executable, "-m", "macf.supervisor",
        "_run_loop",
        "--name", name,
        "--delay", str(restart_delay),
        "--",
    ] + cmd_args

    escaped_cmd = " ".join(
        arg.replace("\\", "\\\\").replace('"', '\\"')
        for arg in supervisor_cmd
    )

    system = platform.system()
    if system == "Darwin":
        # Resolve terminal choice
        if terminal == "auto":
            # Prefer iTerm2 if running, else Terminal.app
            try:
                result = subprocess.run(
                    ["osascript", "-e", 'tell application "System Events" to (name of processes) contains "iTerm2"'],
                    capture_output=True, text=True, timeout=3
                )
                terminal = "iterm2" if "true" in result.stdout.lower() else "terminal"
            except Exception:
                terminal = "terminal"

        if terminal == "iterm2":
            osascript = f'''
                tell application "iTerm2"
                    activate
                    create window with default profile command "{escaped_cmd}"
                end tell
            '''
        else:
            osascript = f'''
                tell application "Terminal"
                    activate
                    do script "{escaped_cmd}"
                end tell
            '''

        subprocess.Popen(["osascript", "-e", osascript])
        time.sleep(1.5)

    elif system == "Linux":
        # Linux: portable terminal detection cascade
        # 1. $TERMINAL env var (user's explicit preference)
        # 2. x-terminal-emulator (Debian/Ubuntu alternatives system)
        # 3. xdg-terminal-exec (new XDG standard, freedesktop.org 2024+)
        # 4. Hardcoded fallback list (last resort)
        term_candidates = []

        env_terminal = os.environ.get("TERMINAL")
        if env_terminal:
            term_candidates.append(env_terminal)

        term_candidates.extend([
            "x-terminal-emulator",  # Debian alternatives → resolves to system default
            "xdg-terminal-exec",    # XDG standard (emerging)
            "gnome-terminal",       # GNOME
            "lxterminal",           # LXDE / RPi OS
            "foot",                 # Wayland / wlroots
            "xterm",                # X11 fallback
            "konsole",              # KDE
        ])

        launched = False
        for term in term_candidates:
            if subprocess.run(["which", term], capture_output=True).returncode == 0:
                if term == "gnome-terminal":
                    subprocess.Popen([term, "--", *supervisor_cmd])
                elif term == "xdg-terminal-exec":
                    subprocess.Popen([term, *supervisor_cmd])
                else:
                    subprocess.Popen([term, "-e", " ".join(supervisor_cmd)])
                time.sleep(1.5)
                launched = True
                break

        if not launched:
            print("No terminal emulator found. Run directly:", file=sys.stderr)
            print(f"  {' '.join(supervisor_cmd)}", file=sys.stderr)
            return 1
    else:
        print(f"Unsupported platform: {system}", file=sys.stderr)
        return 1

    # Find the newly created registry entry
    if REGISTRY_DIR.exists():
        entries = sorted(REGISTRY_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if entries:
            data = json.loads(entries[0].read_text())
            pid = data.get("supervisor_pid", "?")
            print(f"[auto-restart] Launched '{name}' in new terminal (supervisor PID: {pid})")
            print(f"[auto-restart] Command: {' '.join(cmd_args)}")
            print(f"[auto-restart] Manage: macf_tools auto-restart list")
            return pid

    print(f"[auto-restart] Launched '{name}' in new terminal")
    return 0


def run_loop(cmd_args: list, name: str = "", restart_delay: int = 5):
    """Run the supervisor loop (called inside the new terminal).

    This is the actual supervisor process — manages the child.
    """
    pid = os.getpid()
    created = time.time()

    _write_registry(pid, {
        "supervisor_pid": pid,
        "name": name,
        "command": cmd_args,
        "created": created,
        "created_iso": datetime.fromtimestamp(created).isoformat(),
        "restart_count": 0,
        "status": "running",
        "last_restart": None,
        "child_pid": None,
    })

    child = None
    restart_count = 0
    stop_requested = False  # Flag for Ctrl-C during countdown

    def handle_restart(signum, frame):
        nonlocal child
        if child and child.poll() is None:
            _notify_telegram(
                f"Process: {name}\nRestart #{restart_count + 1}",
                prefix="\U0001f504 \u03bcC Triggered"
            )
            child.send_signal(signal.SIGINT)

    def handle_disable(signum, frame):
        _update_registry(pid, status="disabled")
        nonlocal child
        if child and child.poll() is None:
            child.send_signal(signal.SIGINT)

    def handle_sigint_countdown(signum, frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGUSR1, handle_restart)
    signal.signal(signal.SIGUSR2, handle_disable)

    print(f"[auto-restart] Supervisor PID: {pid}")
    print(f"[auto-restart] Name: {name}")
    print(f"[auto-restart] Command: {' '.join(cmd_args)}")
    print(f"[auto-restart] Restart delay: {restart_delay}s")
    print(f"[auto-restart] Remote restart: macf_tools auto-restart restart {pid}")
    print(f"[auto-restart] Disable: macf_tools auto-restart disable {pid}")
    print()

    _notify_telegram(
        f"Name: {name}\nCommand: {' '.join(cmd_args)}\nPID: {pid}",
        prefix="\U0001f680 Supervisor Started"
    )

    try:
        while True:
            reg = _read_registry(pid)
            if reg.get("status") == "disabled":
                print("[auto-restart] Disabled. Exiting.")
                break

            # Use shell=True to resolve aliases (e.g., claude_autoupdating)
            # Interactive shell (-i) needed to source alias definitions from rc files
            cmd_string = " ".join(cmd_args)
            user_shell = os.environ.get("SHELL", "")
            if not user_shell:
                # Cross-platform default: zsh on macOS, bash on Linux
                user_shell = "/bin/zsh" if platform.system() == "Darwin" else "/bin/bash"
            child = subprocess.Popen(
                [user_shell, "-ic", cmd_string]
            )
            _update_registry(pid, child_pid=child.pid, status="running")

            exit_code = child.wait()
            child = None
            restart_count += 1

            _update_registry(pid,
                             restart_count=restart_count,
                             last_restart=time.time(),
                             child_pid=None,
                             last_exit_code=exit_code)

            # Check if disabled during child run
            reg = _read_registry(pid)
            if reg.get("status") == "disabled":
                print(f"[auto-restart] Disabled. Not restarting.")
                break

            # Install countdown SIGINT handler (interactive shell corrupts default handler)
            stop_requested = False
            signal.signal(signal.SIGINT, handle_sigint_countdown)

            print(f"\n[auto-restart] Exited (code {exit_code}). Restart #{restart_count}.")
            print(f"[auto-restart] Ctrl-C during countdown to stop (will NOT restart).\n")
            _notify_telegram(
                f"Process: {name}\nExit code: {exit_code}\nRestart #{restart_count}",
                prefix="\U0001f504 Auto-Restart"
            )

            # Countdown with visual trail (polling flag instead of catching KeyboardInterrupt)
            for remaining in range(restart_delay, 0, -1):
                if stop_requested:
                    break
                print(f"[auto-restart] Restarting in {remaining}s...", flush=True)
                # Poll in short intervals so flag is checked promptly
                for _ in range(10):
                    if stop_requested:
                        break
                    time.sleep(0.1)

            if stop_requested:
                print(f"\n[auto-restart] Ctrl-C caught. Stopping auto-restart.")
                _notify_telegram(
                    f"Process: {name}\nStopped by Ctrl-C during countdown",
                    prefix="\U0001f6d1 Supervisor Stopped"
                )
                break

    except KeyboardInterrupt:
        if child and child.poll() is None:
            child.send_signal(signal.SIGINT)
            child.wait()
    finally:
        _update_registry(pid, status="stopped",
                         stopped=time.time(),
                         total_restarts=restart_count)
        _notify_telegram(
            f"Process: {name}\nRestarts: {restart_count}\nUptime: {_format_duration(time.time() - created)}",
            prefix="\U0001f6d1 Supervisor Stopped"
        )


def list_processes(show_all: bool = False):
    """List managed processes with stats.

    Default: show only running processes.
    --all: show all including stopped/dead (history).
    Auto-cleans stale entries that are not running.
    """
    if not REGISTRY_DIR.exists():
        print("No managed processes.")
        return

    entries = sorted(REGISTRY_DIR.glob("*.json"))
    if not entries:
        print("No managed processes.")
        return

    # Categorize entries
    active = []
    stale = []
    for entry in entries:
        if entry.name == "supervisor_crash.log":
            continue
        data = json.loads(entry.read_text())
        pid = data.get("supervisor_pid", 0)
        alive = _is_alive(pid)
        data["_alive"] = alive
        data["_path"] = entry
        if alive:
            active.append(data)
        else:
            stale.append(data)

    # Auto-clean stale entries (unless --all requested)
    if not show_all:
        for data in stale:
            data["_path"].unlink(missing_ok=True)
        if not active:
            cleaned = len(stale)
            msg = f"No running processes."
            if cleaned:
                msg += f" (cleaned {cleaned} stale entries)"
            print(msg)
            return
        display = active
    else:
        display = active + stale

    # Header
    print(f"{'PID':>8}  {'NAME':<20}  {'STATUS':<10}  {'RESTARTS':>8}  {'UPTIME':>8}  {'COMMAND'}")
    print("-" * 90)

    for data in display:
        pid = data.get("supervisor_pid", 0)
        name = data.get("name", "?")
        status = data.get("status", "?")
        alive = data.get("_alive", False)
        restarts = data.get("restart_count", 0)
        created = data.get("created", 0)
        cmd = " ".join(data.get("command", []))

        # Normalize: dead and stopped both mean "not running"
        if not alive and status in ("running", "dead"):
            status = "stopped"

        uptime = _format_duration(time.time() - created) if created else "?"

        # Color status
        if status == "running" and alive:
            status_display = f"\033[32m{status}\033[0m"
        elif status == "disabled":
            status_display = f"\033[33m{status}\033[0m"
        elif status == "killed":
            status_display = f"\033[31m{status}\033[0m"
        else:
            status_display = f"\033[2m{status}\033[0m"  # dim for stopped

        print(f"{pid:>8}  {name:<20}  {status_display:<21}  {restarts:>8}  {uptime:>8}  {cmd[:40]}")


def restart(pid: int):
    """Send restart signal to a supervised process."""
    if not _is_alive(pid):
        print(f"[auto-restart] Process {pid} is not running")
        return
    os.kill(pid, signal.SIGUSR1)
    print(f"[auto-restart] Restart signal sent to {pid}")


def disable(pid: int):
    """Disable auto-restart for a supervised process."""
    if not _is_alive(pid):
        print(f"[auto-restart] Process {pid} is not running")
        _update_registry(pid, status="disabled")
        return
    os.kill(pid, signal.SIGUSR2)
    print(f"[auto-restart] Disable signal sent to {pid}")


def kill_process(pid: int):
    """Nuclear option: kill supervisor and child processes."""
    data = _read_registry(pid)
    if not data:
        print(f"[auto-restart] No registry entry for PID {pid}")
        return

    child_pid = data.get("child_pid")
    killed = []

    # Kill child first
    if child_pid and _is_alive(child_pid):
        os.kill(child_pid, signal.SIGKILL)
        killed.append(f"child {child_pid}")

    # Kill supervisor
    if _is_alive(pid):
        os.kill(pid, signal.SIGKILL)
        killed.append(f"supervisor {pid}")

    # Clean up registry
    _update_registry(pid, status="killed")

    if killed:
        print(f"[auto-restart] Killed: {', '.join(killed)}")
        _notify_telegram(
            f"Process: {data.get('name', '?')}\nKilled: {', '.join(killed)}",
            prefix="\U0001f480 Process Killed"
        )
    else:
        print(f"[auto-restart] Process {pid} already dead")


def status(pid: int):
    """Show detailed status for a supervised process."""
    data = _read_registry(pid)
    if not data:
        print(f"[auto-restart] No registry entry for PID {pid}")
        return

    alive = _is_alive(pid)
    print(f"Supervisor PID:  {pid} ({'alive' if alive else 'dead'})")
    print(f"Name:            {data.get('name', '?')}")
    print(f"Command:         {' '.join(data.get('command', []))}")
    print(f"Status:          {data.get('status', '?')}")
    print(f"Child PID:       {data.get('child_pid', 'none')}")
    print(f"Created:         {data.get('created_iso', '?')}")
    print(f"Restarts:        {data.get('restart_count', 0)}")
    print(f"Last exit code:  {data.get('last_exit_code', 'N/A')}")
    created = data.get("created", 0)
    if created:
        print(f"Uptime:          {_format_duration(time.time() - created)}")


# Entry point for running inside new terminal
if __name__ == "__main__":
    import argparse
    import traceback

    LOG_FILE = REGISTRY_DIR / "supervisor_crash.log"

    try:
        # Split argv on -- : supervisor args before, command after
        argv = sys.argv[1:]
        if "--" in argv:
            split_idx = argv.index("--")
            supervisor_argv = argv[:split_idx]
            cmd = argv[split_idx + 1:]
        else:
            supervisor_argv = argv
            cmd = []

        parser = argparse.ArgumentParser(description="Auto-restart supervisor")
        parser.add_argument("action", choices=["_run_loop"])
        parser.add_argument("--name", default="")
        parser.add_argument("--delay", type=int, default=2)

        args = parser.parse_args(supervisor_argv)

        if args.action == "_run_loop":
            run_loop(cmd, name=args.name, restart_delay=args.delay)

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"\n[auto-restart] CRASH:\n{error_msg}", file=sys.stderr)
        # Log to file (persists after window closes)
        REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(f"\n--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
            f.write(f"argv: {sys.argv}\n")
            f.write(error_msg)
        print(f"[auto-restart] Crash log: {LOG_FILE}")
        print("[auto-restart] Press Enter to close...")
        try:
            input()
        except (EOFError, KeyboardInterrupt):
            pass
