# tools/src/maceff/cli.py
import argparse, json, os, subprocess, sys, glob, platform, socket
from pathlib import Path
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    from importlib.metadata import version
    _ver = version("macf")
except Exception:
    _ver = "0.0.0"

from .config import ConsciousnessConfig
from .hooks.compaction import detect_compaction, inject_recovery
from .agent_events_log import append_event
from .event_queries import get_cycle_number_from_events
from .task.reader import TaskReader
from .utils import (
    get_current_session_id,
    get_dev_scripts_dir,
    get_formatted_timestamp,
    get_token_info,
    extract_current_git_hash,
    get_claude_code_version,
    get_temporal_context,
    detect_auto_mode,
    find_agent_home,
    get_env_var_report,
    get_agent_identity,
    find_project_root,
    find_maceff_root,
    get_macf_package_path,
    get_hooks_dir,
    get_total_context,
)
from .utils.environment import detect_model

# -------- ANSI escape codes --------
ANSI_RESET = "\033[0m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_DIM = "\033[2m"
ANSI_STRIKE = "\033[9m"


def _dim_task_ids(text: str) -> str:
    """Wrap task ID patterns (#N and [^#N]) in dim ANSI codes."""
    import re
    # Pattern matches: #123 at start, or [^#123] anywhere
    # Replace #N at start with dim version
    text = re.sub(r'^(#\d+)', f'{ANSI_DIM}\\1{ANSI_RESET}', text)
    # Replace [^#N] with dim version
    text = re.sub(r'(\[\^#\d+\])', f'{ANSI_DIM}\\1{ANSI_RESET}', text)
    return text


def _strip_ansi(text: str) -> str:
    """Strip all ANSI escape codes from text."""
    import re
    return re.sub(r'\x1b\[[0-9;]*m', '', text)


# -------- helpers --------
def _pick_tz():
    """Prefer MACEFF_TZ, then TZ, else system local; fall back to UTC."""
    for key in ("MACEFF_TZ", "TZ"):
        name = os.getenv(key)
        if name and ZoneInfo is not None:
            try:
                return ZoneInfo(name)
            except Exception:
                pass
    try:
        return datetime.now().astimezone().tzinfo or timezone.utc
    except Exception:
        return timezone.utc

def _now_iso(tz=None):
    tz = tz or _pick_tz()
    return datetime.now(tz).replace(microsecond=0).isoformat()

def _format_time_ago(file_path: Path) -> str:
    """Format time ago string for a file."""
    try:
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=_pick_tz())
        now = datetime.now(_pick_tz())
        delta = now - mtime
        hours = int(delta.total_seconds() // 3600)
        minutes = int((delta.total_seconds() % 3600) // 60)
        return f"{hours}h {minutes}m ago"
    except Exception:
        return "unknown"

# -------- commands --------
def cmd_tree(args: argparse.Namespace, root_parser: argparse.ArgumentParser = None) -> int:
    """Print command tree by introspecting argparse parser structure.

    Modeled after unix 'tree' command - minimal token output showing
    subcommand structure with usage strings at leaves.

    Uses argparse internal attributes:
    - parser._actions to find all actions
    - isinstance(action, argparse._SubParsersAction) to identify subparsers
    - action.choices to get {name: parser} mapping
    """
    if root_parser is None:
        # Parser will be injected by main() after construction
        print("Error: Parser not available", file=sys.stderr)
        return 1

    def get_args_string(parser: argparse.ArgumentParser) -> str:
        """Build args string from parser actions (cleaner than parsing usage).

        Distinguishes required from optional args and renders mutually
        exclusive required groups with (--a A | --b B) notation.
        """
        parts = []

        # Collect actions that belong to required mutually exclusive groups
        mutex_actions = set()
        mutex_groups = []
        for group in parser._mutually_exclusive_groups:
            if group.required:
                group_parts = []
                for action in group._group_actions:
                    mutex_actions.add(id(action))
                    opts = action.option_strings[0] if action.option_strings else action.dest
                    meta = action.metavar or action.dest.upper()
                    if action.nargs == 0:
                        group_parts.append(opts)
                    else:
                        group_parts.append(f"{opts} {meta}")
                mutex_groups.append(f"({' | '.join(group_parts)})")

        for action in parser._actions:
            if isinstance(action, argparse._HelpAction):
                continue
            if isinstance(action, argparse._SubParsersAction):
                continue
            if id(action) in mutex_actions:
                continue  # Rendered as group below
            if action.option_strings:
                opts = action.option_strings[0]
                if action.nargs == 0:
                    parts.append(f"[{opts}]")
                elif action.required:
                    parts.append(f"{opts} {action.metavar or action.dest.upper()}")
                else:
                    parts.append(f"[{opts} {action.metavar or action.dest.upper()}]")
            else:
                name = action.metavar or action.dest
                if action.nargs in ('?', '*'):
                    parts.append(f"[{name}]")
                elif action.nargs == '+':
                    parts.append(f"{name} [{name} ...]")
                else:
                    parts.append(name)

        # Insert mutex groups after required positional/named args, before optional flags
        return ' '.join(mutex_groups + parts)

    def get_subparsers(parser: argparse.ArgumentParser) -> dict:
        """Get {name: parser} dict of subcommands from parser."""
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                return dict(action.choices)
        return {}

    def print_tree(parser: argparse.ArgumentParser, prefix: str = "", name: str = "macf_tools", is_last: bool = True):
        """Recursively print parser tree in unix tree format."""
        # Connector characters
        connector = "└── " if is_last else "├── "
        extension = "    " if is_last else "│   "

        # Get subparsers for this parser
        subparsers = get_subparsers(parser)

        if subparsers:
            # Has subcommands - print name only
            print(f"{prefix}{connector}{name}")
            # Recurse into subcommands
            items = sorted(subparsers.items())
            for i, (subcmd_name, subcmd_parser) in enumerate(items):
                is_last_child = (i == len(items) - 1)
                print_tree(subcmd_parser, prefix + extension, subcmd_name, is_last_child)
        else:
            # Leaf node - print name with args
            args_str = get_args_string(parser)
            if args_str:
                print(f"{prefix}{connector}{name} {args_str}")
            else:
                print(f"{prefix}{connector}{name}")

    print("macf_tools")
    subparsers = get_subparsers(root_parser)
    items = sorted(subparsers.items())
    for i, (name, parser) in enumerate(items):
        is_last = (i == len(items) - 1)
        print_tree(parser, "", name, is_last)

    return 0


def cmd_env(args: argparse.Namespace) -> int:
    """Print comprehensive environment summary."""
    temporal = get_temporal_context()
    session_id = get_current_session_id()

    # Get agent home path
    try:
        agent_home = find_agent_home()
    except Exception:
        agent_home = None

    # Count installed hooks (in .claude/hooks/)
    hooks_dir = agent_home / ".claude" / "hooks" if agent_home else None
    hooks_count = len(list(hooks_dir.glob("*.py"))) if hooks_dir and hooks_dir.exists() else 0

    # Get auto mode status
    auto_enabled, _ = detect_auto_mode(session_id)

    # Resolve paths safely
    def resolve_path(p):
        try:
            return str(p.resolve()) if p and p.exists() else str(p) if p else "(not set)"
        except Exception:
            return str(p) if p else "(not set)"

    # Get agent identity
    agent_identity = get_agent_identity()

    # Compute CC internal paths
    try:
        project_root = find_project_root()
        # Encode project path (/ → -)
        encoded_path = str(project_root).replace("/", "-")
        cc_project_dir = Path.home() / ".claude" / "projects" / encoded_path
    except Exception:
        cc_project_dir = None

    # CC Tasks path (use TaskReader for session detection)
    try:
        reader = TaskReader()
        cc_tasks_dir = reader.session_path if reader.session_path else None
    except Exception:
        cc_tasks_dir = None

    # Get framework paths
    try:
        macf_package = get_macf_package_path()
    except Exception:
        macf_package = None

    try:
        maceff_root = find_maceff_root()
        framework_dir = maceff_root / "framework" if maceff_root else None
    except Exception:
        framework_dir = None

    # Gather all data
    data = {
        "identity": {
            "agent_id": agent_identity
        },
        "versions": {
            "macf": _ver,
            "claude_code": get_claude_code_version() or "(unavailable)",
            "model": detect_model(),
            "context_window": f"{get_total_context():,}",
            "python_path": sys.executable,
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        },
        "time": {
            "local": temporal.get("timestamp_formatted", _now_iso()),
            "utc": datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
            "timezone": temporal.get("timezone", "UTC")
        },
        "paths": {
            "agent_home": resolve_path(agent_home),
            "event_log": resolve_path(agent_home / ".maceff" / "agent_events_log.jsonl") if agent_home else "(not set)",
            "hooks_dir": resolve_path(hooks_dir),
            "checkpoints_dir": resolve_path(agent_home / "agent" / "private" / "checkpoints") if agent_home else "(not set)",
            "settings_local": resolve_path(agent_home / ".claude" / "settings.local.json") if agent_home else "(not set)"
        },
        "cc_internal": {
            "cc_project_dir": resolve_path(cc_project_dir),
            "cc_tasks_dir": resolve_path(cc_tasks_dir)
        },
        "framework": {
            "macf_package": resolve_path(macf_package),
            "framework_dir": resolve_path(framework_dir)
        },
        "session": {
            "session_id": session_id or "(unknown)",
            "cycle": get_cycle_number_from_events(),
            "git_hash": extract_current_git_hash() or "(unknown)"
        },
        "system": {
            "platform": platform.system().lower(),
            "os_version": f"{platform.system()} {platform.release()}",
            "cwd": str(Path.cwd().resolve()),
            "hostname": socket.gethostname()
        },
        "environment": get_env_var_report(),
        "config": {
            "hooks_installed": hooks_count,
            "auto_mode": auto_enabled
        }
    }

    # Output format
    if getattr(args, 'json', False):
        # Convert tuple to dict for JSON serialization
        key_vars, extra_vars = data['environment']
        data['environment'] = {"key": key_vars, "extra": extra_vars}
        print(json.dumps(data, indent=2))
    else:
        # Pretty-print format
        line = "━" * 80
        print(line)

        print("Agent ID")
        print(f"  {data['identity']['agent_id']}")
        print()

        print("Versions")
        print(f"  MACF:         {data['versions']['macf']}")
        print(f"  Claude Code:  {data['versions']['claude_code']}")
        print(f"  Model:        {data['versions']['model']}")
        print(f"  Context:      {data['versions']['context_window']} tokens")
        print(f"  Python:       {data['versions']['python_path']} ({data['versions']['python_version']})")
        print()

        print("Time")
        print(f"  Local:        {data['time']['local']}")
        print(f"  UTC:          {data['time']['utc']}")
        print(f"  Timezone:     {data['time']['timezone']}")
        print()

        print("Paths")
        print(f"  Agent Home:   {data['paths']['agent_home']}")
        print(f"  Event Log:    {data['paths']['event_log']}")
        print(f"  Hooks Dir:    {data['paths']['hooks_dir']}")
        print(f"  Checkpoints:  {data['paths']['checkpoints_dir']}")
        print(f"  Settings:     {data['paths']['settings_local']}")
        print()

        print("Claude Code Internal")
        print(f"  CC Project Dir: {data['cc_internal']['cc_project_dir']}")
        print(f"  CC Tasks Dir:   {data['cc_internal']['cc_tasks_dir']}")
        print()

        print("Framework")
        print(f"  MACF Package:   {data['framework']['macf_package']}")
        print(f"  Framework Dir:  {data['framework']['framework_dir']}")
        print()

        print("Session")
        print(f"  Session ID:   {data['session']['session_id']}")
        print(f"  Cycle:        {data['session']['cycle']}")
        print(f"  Git Hash:     {data['session']['git_hash']}")
        print()

        print("System")
        print(f"  Platform:     {data['system']['platform']}")
        print(f"  OS:           {data['system']['os_version']}")
        print(f"  CWD:          {data['system']['cwd']}")
        print(f"  Hostname:     {data['system']['hostname']}")
        print()

        print("Environment")
        key_vars, extra_vars = data['environment']
        for k, v in key_vars.items():
            print(f"  {k}: {v}")
        if extra_vars:
            print("  ---")
            for k, v in extra_vars.items():
                print(f"  {k}: {v}")
        print()

        print("Config")
        print(f"  Hooks Installed: {data['config']['hooks_installed']}")
        print(f"  Auto Mode:       {data['config']['auto_mode']}")

        print(line)

    return 0

def cmd_time(_: argparse.Namespace) -> int:
    current_time = _now_iso()
    print(current_time)

    # Show gap since most recent CCP
    try:
        config = ConsciousnessConfig()
        checkpoints_path = config.get_checkpoints_path()
        if checkpoints_path.exists():
            # Find CCP files (multiple patterns for consciousness checkpoints)
            ccp_patterns = ["*_ccp.md", "*_CCP.md", "*_checkpoint.md"]
            ccp_files = []
            for pattern in ccp_patterns:
                ccp_files.extend(checkpoints_path.glob(pattern))
            ccp_files = sorted(ccp_files, key=lambda p: p.stat().st_mtime, reverse=True)
            if ccp_files:
                latest_ccp = ccp_files[0]
                ccp_mtime = datetime.fromtimestamp(latest_ccp.stat().st_mtime, tz=_pick_tz())
                now = datetime.now(_pick_tz())
                delta = now - ccp_mtime
                hours = int(delta.total_seconds() // 3600)
                minutes = int((delta.total_seconds() % 3600) // 60)
                print(f"Last CCP: {latest_ccp.name} ({hours}h {minutes}m ago)")
    except Exception:
        # Graceful fallback if CCP lookup fails
        pass

    return 0

def cmd_budget(_: argparse.Namespace) -> int:
    warn = float(os.getenv("MACEFF_TOKEN_WARN", "0.85"))
    hard = float(os.getenv("MACEFF_TOKEN_HARD", "0.95"))
    mode = os.getenv("MACEFF_BUDGET_MODE", "concise/default")
    payload = {"mode": mode, "thresholds": {"warn": warn, "hard": hard}}
    used = os.getenv("MACEFF_TOKEN_USED")
    if used is not None:
        try:
            payload["used"] = float(used)
        except ValueError:
            pass
    print(json.dumps(payload, indent=2))
    return 0

def cmd_list_ccps(args: argparse.Namespace) -> int:
    """List consciousness checkpoints with timestamps."""
    try:
        config = ConsciousnessConfig()
        checkpoints_path = config.get_checkpoints_path()

        if not checkpoints_path.exists():
            print("No checkpoints directory found")
            return 0

        # Find CCP files (multiple patterns for consciousness checkpoints)
        ccp_patterns = ["*_ccp.md", "*_CCP.md", "*_checkpoint.md"]
        ccp_files = []
        for pattern in ccp_patterns:
            ccp_files.extend(checkpoints_path.glob(pattern))
        ccp_files = sorted(ccp_files, key=lambda p: p.stat().st_mtime, reverse=True)

        if not ccp_files:
            print("No consciousness checkpoints found")
            return 0

        # Apply --recent limit if specified
        recent = getattr(args, 'recent', None)
        if recent is not None:
            ccp_files = ccp_files[:recent]

        for ccp_file in ccp_files:
            time_ago = _format_time_ago(ccp_file)
            print(f"{ccp_file.name} ({time_ago})")

    except Exception as e:
        print(f"Error listing CCPs: {e}")
        return 1

    return 0

def cmd_session_info(args: argparse.Namespace) -> int:
    """Show session information as JSON."""
    try:
        config = ConsciousnessConfig()
        session_id = get_current_session_id()

        # Get temp directory path using unified utils
        temp_dir = get_dev_scripts_dir(session_id)

        data = {
            "session_id": session_id,
            "agent_name": config.agent_name,
            "agent_id": config.agent_id,
            "agent_root": str(config.agent_root),
            "cwd": str(Path.cwd()),
            "temp_directory": str(temp_dir) if temp_dir else "unavailable",
            "checkpoints_path": str(config.get_checkpoints_path()),
            "reflections_path": str(config.get_reflections_path())
        }

        print(json.dumps(data, indent=2))

    except Exception as e:
        print(f"Error getting session info: {e}")
        return 1

    return 0


def _update_settings_file(settings_path: Path, hooks_prefix: str) -> bool:
    """Update settings.json with hooks configuration, merging existing settings."""
    try:
        # Load existing settings or create new
        if settings_path.exists():
            with open(settings_path) as f:
                settings = json.load(f)
        else:
            settings = {}

        # Ensure hooks section exists
        if "hooks" not in settings:
            settings["hooks"] = {}

        # All 10 hooks with their script names
        hook_configs = [
            ("SessionStart", "session_start.py"),
            ("UserPromptSubmit", "user_prompt_submit.py"),
            ("Stop", "stop.py"),
            ("SubagentStop", "subagent_stop.py"),
            ("PreToolUse", "pre_tool_use.py"),
            ("PostToolUse", "post_tool_use.py"),
            ("SessionEnd", "session_end.py"),
            ("PreCompact", "pre_compact.py"),
            ("PermissionRequest", "permission_request.py"),
            ("Notification", "notification.py"),
        ]

        # Register all hooks
        for hook_name, script_name in hook_configs:
            settings["hooks"][hook_name] = [
                {
                    "matcher": "",
                    "hooks": [
                        {
                            "type": "command",
                            "command": f"{hooks_prefix}/{script_name}"
                        }
                    ]
                }
            ]

        # Merge permissions from template (grant commands require "ask")
        try:
            maceff_root = find_maceff_root()
            template_path = maceff_root / "framework" / "templates" / "settings.permissions.json"
            if template_path.exists():
                with open(template_path) as f:
                    perm_template = json.load(f)
                if "permissions" in perm_template:
                    if "permissions" not in settings:
                        settings["permissions"] = {}
                    # Merge "ask" permissions
                    if "ask" in perm_template["permissions"]:
                        if "ask" not in settings["permissions"]:
                            settings["permissions"]["ask"] = []
                        for perm in perm_template["permissions"]["ask"]:
                            if perm not in settings["permissions"]["ask"]:
                                settings["permissions"]["ask"].append(perm)
        except Exception as e:
            print(f"   Warning: Could not merge permissions template: {e}", file=sys.stderr)

        # Backup existing file
        if settings_path.exists():
            backup_path = settings_path.with_suffix('.json.backup')
            settings_path.rename(backup_path)
            print(f"   Backed up existing settings to: {backup_path}")

        # Write updated settings
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        with open(settings_path, 'w') as f:
            json.dump(settings, f, indent=2)

        return True

    except Exception as e:
        print(f"Error updating settings: {e}")
        return False


def _check_hooks_in_settings(settings_path: Path) -> bool:
    """Check if hooks section exists in a settings file."""
    try:
        if not settings_path.exists():
            return False
        with open(settings_path) as f:
            settings = json.load(f)
        return bool(settings.get("hooks"))
    except Exception:
        return False


def _clear_hooks_from_settings(settings_path: Path) -> bool:
    """Remove hooks section from a settings file to prevent duplicate execution."""
    try:
        if not settings_path.exists():
            return True

        with open(settings_path) as f:
            settings = json.load(f)

        if "hooks" not in settings:
            return True

        del settings["hooks"]

        with open(settings_path, 'w') as f:
            json.dump(settings, f, indent=2)

        return True
    except Exception as e:
        print(f"Warning: Could not clear hooks from {settings_path}: {e}")
        return False


def cmd_hook_install(args: argparse.Namespace) -> int:
    """Install all 10 consciousness hooks with local/global mode selection.

    IDEMPOTENT: Always clears hooks from the OTHER location to prevent duplicate execution.
    If switching modes, prompts for confirmation.
    """
    try:
        # Container detection (FP#27 fix - check /.dockerenv directly)
        in_container = Path("/.dockerenv").exists()

        # Define both settings paths
        global_settings = Path.home() / ".claude" / "settings.json"
        local_settings = Path.cwd() / ".claude" / "settings.local.json"

        # Check current state
        has_global_hooks = _check_hooks_in_settings(global_settings)
        has_local_hooks = _check_hooks_in_settings(local_settings)

        # Determine installation mode
        if in_container:
            # Container: force global mode, no interactive prompt (FP#27)
            mode = 'global'
        elif hasattr(args, 'global_install') and args.global_install:
            mode = 'global'
        elif hasattr(args, 'local_install') and args.local_install:
            mode = 'local'
        else:
            # Interactive mode (host only)
            print("\nWhere do you want to install hooks?")
            print("[1] Local project (.claude/hooks/) [DEFAULT]")
            print("[2] Global user directory (~/.claude/hooks/)")
            choice = input("\nPress Enter for [1], or enter choice: ").strip() or "1"
            mode = 'global' if choice == '2' else 'local'

        # Check if switching modes (hooks exist in opposite location)
        switching_to_global = (mode == 'global' and has_local_hooks)
        switching_to_local = (mode == 'local' and has_global_hooks)

        if switching_to_global or switching_to_local:
            other_loc = "local (.claude/settings.local.json)" if switching_to_global else "global (~/.claude/settings.json)"
            print(f"\n⚠️  Hooks currently exist in {other_loc}")
            print(f"   Installing to {'global' if mode == 'global' else 'local'} will REMOVE hooks from {other_loc}")
            confirm = input("   Continue? [y/N]: ").strip().lower()
            if confirm != 'y':
                print("❌ Cancelled")
                return 1

        # Clear hooks from the OTHER location (always, to ensure no duplicates)
        if mode == 'local':
            if has_global_hooks:
                print(f"   Clearing hooks from global settings...")
                _clear_hooks_from_settings(global_settings)
        else:  # global
            if has_local_hooks:
                print(f"   Clearing hooks from local settings...")
                _clear_hooks_from_settings(local_settings)

        # Set paths based on mode and environment
        if mode == 'global':
            hooks_dir = Path.home() / ".claude" / "hooks"
            settings_file = Path.home() / ".claude" / "settings.json"
            if in_container:
                # Container: absolute venv Python + absolute hook paths (FP#27)
                hooks_prefix = f"/opt/maceff-venv/bin/python {Path.home()}/.claude/hooks"
            else:
                hooks_prefix = "python ~/.claude/hooks"
        else:
            # Local mode (host only - container always uses global)
            hooks_dir = Path.cwd() / ".claude" / "hooks"
            settings_file = Path.cwd() / ".claude" / "settings.local.json"
            hooks_prefix = "python .claude/hooks"

        # Create hooks directory
        hooks_dir.mkdir(parents=True, exist_ok=True)

        # All 10 hooks with their handler module names
        hooks_to_install = [
            ("session_start.py", "handle_session_start"),
            ("user_prompt_submit.py", "handle_user_prompt_submit"),
            ("stop.py", "handle_stop"),
            ("subagent_stop.py", "handle_subagent_stop"),
            ("pre_tool_use.py", "handle_pre_tool_use"),
            ("post_tool_use.py", "handle_post_tool_use"),
            ("session_end.py", "handle_session_end"),
            ("pre_compact.py", "handle_pre_compact"),
            ("permission_request.py", "handle_permission_request"),
            ("notification.py", "handle_notification"),
        ]

        # Find installed package location for handler modules
        import macf.hooks as hooks_package
        package_hooks_dir = Path(hooks_package.__file__).parent

        # Create symlinks to handler modules
        for script_name, handler_module in hooks_to_install:
            hook_script = hooks_dir / script_name
            handler_path = package_hooks_dir / f"{handler_module}.py"

            # Remove existing file/symlink if present
            if hook_script.exists() or hook_script.is_symlink():
                hook_script.unlink()

            # Create symlink to handler module
            hook_script.symlink_to(handler_path)

        # Update settings file
        if _update_settings_file(settings_file, hooks_prefix):
            print(f"\n✅ All 10 hooks installed successfully!")
            print(f"   Mode: {mode}")
            print(f"   Directory: {hooks_dir}")
            print(f"   Settings: {settings_file}")
            print(f"\n   Hooks installed:")
            for script_name, _ in hooks_to_install:
                print(f"   - {script_name}")
            print(f"\nConsciousness infrastructure will activate on next session.")
            return 0
        else:
            print(f"\n❌ Hook scripts created but settings update failed")
            print(f"   Manually add to {settings_file}")
            return 1

    except Exception as e:
        print(f"Error installing hooks: {e}")
        return 1


def cmd_framework_install(args: argparse.Namespace) -> int:
    """Install framework artifacts (hooks, commands, skills) to .claude directory."""
    try:
        # Determine what to install
        hooks_only = getattr(args, 'hooks_only', False)
        skip_hooks = getattr(args, 'skip_hooks', False)

        # Find framework root using standard path resolution
        maceff_root = find_maceff_root()
        framework_root = maceff_root / "framework"
        if not framework_root.exists():
            print(f"Error: Framework directory not found at {framework_root}")
            print(f"   MacEff root resolved to: {maceff_root}")
            print(f"   Fix: Set MACEFF_ROOT_DIR to your MacEff installation")
            return 1

        claude_dir = Path.cwd() / ".claude"
        commands_dir = claude_dir / "commands"
        skills_dir = claude_dir / "skills"

        installed_count = {"hooks": 0, "commands": 0, "skills": 0}

        # Install hooks (unless skip_hooks or already done via hooks_only)
        if not skip_hooks:
            print("\n📦 Installing hooks...")
            # Reuse existing hook install logic
            hooks_args = argparse.Namespace(local_install=True, global_install=False)
            hook_result = cmd_hook_install(hooks_args)
            if hook_result == 0:
                installed_count["hooks"] = 10
            else:
                print("   Warning: Hook installation had issues")

        if hooks_only:
            print(f"\n✅ Hooks-only installation complete")
            return 0

        # Install commands (symlink maceff*/ namespace directories)
        print("\n📦 Installing commands...")
        commands_src = framework_root / "commands"
        if commands_src.exists():
            commands_dir.mkdir(parents=True, exist_ok=True)
            for cmd_ns in commands_src.glob("maceff*/"):
                if cmd_ns.is_dir():
                    target = commands_dir / cmd_ns.name
                    if target.exists() or target.is_symlink():
                        if target.is_symlink():
                            target.unlink()
                        else:
                            import shutil
                            shutil.rmtree(target)
                    target.symlink_to(cmd_ns)
                    # Count .md files in namespace for reporting
                    md_count = sum(1 for _ in cmd_ns.rglob("*.md"))
                    installed_count["commands"] += md_count
                    print(f"   ✓ {cmd_ns.name}/ ({md_count} commands)")
        else:
            print(f"   No commands directory at {commands_src}")

        # Install skills (symlink maceff-*/ directories)
        print("\n📦 Installing skills...")
        skills_src = framework_root / "skills"
        if skills_src.exists():
            skills_dir.mkdir(parents=True, exist_ok=True)
            for skill_dir in skills_src.glob("maceff-*/"):
                if skill_dir.is_dir():
                    target = skills_dir / skill_dir.name
                    if target.exists() or target.is_symlink():
                        if target.is_symlink():
                            target.unlink()
                        else:
                            import shutil
                            shutil.rmtree(target)
                    target.symlink_to(skill_dir)
                    installed_count["skills"] += 1
                    print(f"   ✓ {skill_dir.name}/")
        else:
            print(f"   No skills directory at {skills_src}")

        # Summary
        print(f"\n✅ Framework installation complete!")
        print(f"   Hooks: {installed_count['hooks']}")
        print(f"   Commands: {installed_count['commands']}")
        print(f"   Skills: {installed_count['skills']}")

        return 0

    except Exception as e:
        print(f"Error installing framework: {e}")
        return 1


def cmd_hook_test(args: argparse.Namespace) -> int:
    """Test compaction detection on current session."""
    try:
        # Find current session JSONL file
        claude_dir = Path.home() / ".claude" / "projects"
        if not claude_dir.exists():
            print("No .claude/projects directory found")
            return 1

        all_jsonl_files = []
        for project_dir in claude_dir.iterdir():
            if project_dir.is_dir():
                jsonl_files = list(project_dir.glob("*.jsonl"))
                all_jsonl_files.extend(jsonl_files)

        if not all_jsonl_files:
            print("No JSONL transcript files found")
            return 1

        # Get most recently modified JSONL file
        latest_file = max(all_jsonl_files, key=lambda p: p.stat().st_mtime)

        print(f"Testing transcript: {latest_file.name}")

        # Check for compaction
        if detect_compaction(latest_file):
            print("✅ COMPACTION DETECTED")
            print(inject_recovery())
        else:
            print("❌ No compaction detected - session appears normal")

    except Exception as e:
        print(f"Error testing hook: {e}")
        return 1

    return 0


def cmd_hook_logs(args: argparse.Namespace) -> int:
    """Display hook event logs."""
    # Get session_id
    session_id = args.session if hasattr(args, 'session') and args.session else get_current_session_id()

    # Get agent_id
    config = ConsciousnessConfig()
    agent_id = config.agent_id

    # Get log path using unified utils
    log_dir = get_hooks_dir(session_id, create=False)
    if not log_dir:
        print(f"No logs found for session: {session_id}")
        return 1

    log_file = log_dir / "hook_events.log"
    if not log_file.exists():
        print(f"No hook events logged yet for session: {session_id}")
        return 0

    # Display logs
    print(f"Hook events for session {session_id} (agent: {agent_id}):\n")

    with open(log_file, 'r') as f:
        for line in f:
            try:
                event = json.loads(line)
                timestamp = event.get('timestamp', 'unknown')
                hook_name = event.get('hook_name', 'unknown')
                event_type = event.get('event_type', 'unknown')

                # Format based on event type
                if event_type == "HOOK_START":
                    print(f"[{timestamp}] {hook_name}: START")
                elif event_type == "HOOK_COMPLETE":
                    duration = event.get('duration_ms', '?')
                    print(f"[{timestamp}] {hook_name}: COMPLETE ({duration}ms)")
                elif event_type == "HOOK_ERROR":
                    error = event.get('error', 'unknown error')
                    print(f"[{timestamp}] {hook_name}: ERROR - {error}")
                elif event_type == "COMPACTION_CHECK":
                    detected = event.get('compaction_detected', False)
                    duration = event.get('duration_ms', '?')
                    print(f"[{timestamp}] {hook_name}: Compaction={'DETECTED' if detected else 'not detected'} ({duration}ms)")
                elif event_type == "TRANSCRIPT_FOUND":
                    transcript_name = event.get('transcript_name', 'unknown')
                    print(f"[{timestamp}] {hook_name}: Found transcript {transcript_name}")
                else:
                    print(f"[{timestamp}] {hook_name}: {event_type}")

            except json.JSONDecodeError:
                print(f"Invalid log entry: {line.strip()}")

    return 0


def cmd_hook_status(args: argparse.Namespace) -> int:
    """Display current hook sidecar states."""
    from .hooks.sidecar import read_sidecar

    # Get session_id
    session_id = get_current_session_id()

    # Get agent_id
    config = ConsciousnessConfig()
    agent_id = config.agent_id

    # Get hooks directory using unified utils
    hooks_dir = get_hooks_dir(session_id, create=False)
    if not hooks_dir:
        print(f"No session directory found for: {session_id}")
        return 1

    print(f"Hook states for session {session_id} (agent: {agent_id}):\n")

    # Find all sidecar files
    sidecar_files = list(hooks_dir.glob("sidecar_*.json"))

    if not sidecar_files:
        print("No hook states recorded yet")
        return 0

    for sidecar_file in sidecar_files:
        hook_name = sidecar_file.stem.replace("sidecar_", "")
        state = read_sidecar(hook_name, session_id)

        print(f"Hook: {hook_name}")
        print(json.dumps(state, indent=2))
        print()

    return 0


def cmd_config_init(args: argparse.Namespace) -> int:
    """Initialize .macf/config.json with interactive prompts."""
    config_dir = Path.cwd() / '.macf'
    config_file = config_dir / 'config.json'

    if config_file.exists() and not args.force:
        print(f"Config file already exists: {config_file}")
        print("Use --force to overwrite")
        return 1

    # Interactive prompts
    print("Initialize MacEff agent configuration\n")
    moniker = input("Agent moniker (e.g., MyAgent): ").strip()
    if not moniker:
        print("Error: Moniker required")
        return 1

    agent_type = input("Agent type [primary_agent]: ").strip() or "primary_agent"
    description = input("Description: ").strip() or f"{moniker} agent"

    # Create config structure
    config = {
        "agent_identity": {
            "moniker": moniker,
            "type": agent_type,
            "description": description
        },
        "logging": {
            "enabled": True,
            "level": "INFO",
            "console_output": False
        },
        "hooks": {
            "capture_output": True,
            "sidecar_enabled": True
        }
    }

    # Write config file
    config_dir.mkdir(parents=True, exist_ok=True)
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"\n✅ Configuration created: {config_file}")
    print(f"   Agent moniker: {moniker}")
    print(f"   Logging paths: /tmp/macf_hooks/{moniker}/{{session_id}}/")

    return 0


def cmd_config_show(args: argparse.Namespace) -> int:
    """Display current configuration."""
    config = ConsciousnessConfig()

    print(f"Agent ID: {config.agent_id}")
    print(f"Agent Name: {config.agent_name}")
    print(f"Agent Root: {config.agent_root}")

    # Determine context
    if config._is_container():
        context = "container"
    elif config._is_host():
        context = "host"
    else:
        context = "fallback"
    print(f"Detection Context: {context}")

    # Load and display full config if available
    config_data = config.load_config()
    if config_data:
        print("\nFull configuration:")
        print(json.dumps(config_data, indent=2))
    else:
        print("\nNo .macf/config.json found (using defaults)")

    # Show computed paths
    print(f"\nComputed paths:")
    print(f"  Checkpoints: {config.get_checkpoints_path()}")
    print(f"  Reflections: {config.get_reflections_path()}")
    print(f"  Logs: /tmp/macf_hooks/{config.agent_id}/{{session_id}}/")

    return 0


def cmd_claude_config_init(args: argparse.Namespace) -> int:
    """Initialize .claude.json with recommended defaults."""
    try:
        settings_path = Path.home() / ".claude.json"

        # Read existing settings or create new
        if settings_path.exists():
            with open(settings_path, 'r') as f:
                settings = json.load(f)
            print(f"Updating existing .claude.json at {settings_path}")
        else:
            settings = {}
            print(f"Creating new .claude.json at {settings_path}")

        # Set recommended defaults
        settings['verbose'] = True
        settings['autoCompactEnabled'] = False

        # Write atomically via temp file
        temp_path = settings_path.with_suffix('.tmp')
        with open(temp_path, 'w') as f:
            json.dump(settings, f, indent=2)
        temp_path.replace(settings_path)

        print("\n✅ Claude Code configuration updated:")
        print("   verbose: true")
        print("   autoCompactEnabled: false")
        print("\nChanges will take effect on next Claude Code session.")

        return 0

    except (OSError, json.JSONDecodeError, TypeError) as e:
        print(f"❌ Error updating .claude.json: {e}")
        return 1


def cmd_claude_config_show(args: argparse.Namespace) -> int:
    """Show current .claude.json configuration."""
    try:
        settings_path = Path.home() / ".claude.json"

        if not settings_path.exists():
            print(f"No .claude.json found at {settings_path}")
            print("\nRun 'macf_tools claude-config init' to create with defaults.")
            return 0

        with open(settings_path, 'r') as f:
            settings = json.load(f)

        print(f"Claude Code Configuration ({settings_path}):\n")
        print(json.dumps(settings, indent=2))

        return 0

    except (OSError, json.JSONDecodeError) as e:
        print(f"❌ Error reading .claude.json: {e}")
        return 1


def cmd_context(args: argparse.Namespace) -> int:
    """Show current token usage and CL (Context Left) level."""
    try:
        # Get session_id from args or use current
        session_id = getattr(args, 'session', None)

        # Get token info
        token_info = get_token_info(session_id=session_id)

        # JSON output mode
        if getattr(args, 'json_output', False):
            print(json.dumps(token_info, indent=2))
            return 0

        # Human-readable format
        tokens_used = token_info['tokens_used']
        tokens_remaining = token_info['tokens_remaining']
        percentage_used = token_info['percentage_used']
        cl_level = token_info['cl_level']
        source = token_info['source']
        total = get_total_context()

        print(f"Token Usage: {tokens_used:,} / {total:,} ({percentage_used:.1f}%)")
        print(f"Remaining: {tokens_remaining:,} tokens")
        print(f"CL Level: {cl_level} (Context Left)")
        print(f"Source: {source}")

        return 0

    except Exception as e:
        print(f"Error getting token info: {e}")
        return 1


def cmd_statusline(args: argparse.Namespace) -> int:
    """Generate formatted statusline for Claude Code display."""
    from .utils.statusline import get_statusline_data, format_statusline

    try:
        # Check for CC JSON on stdin (non-blocking)
        cc_json = None
        if not sys.stdin.isatty():
            try:
                stdin_data = sys.stdin.read().strip()
                if stdin_data:
                    cc_json = json.loads(stdin_data)
            except (json.JSONDecodeError, Exception):
                # Ignore stdin parsing failures - use MACF data only
                pass

        # Gather statusline data
        data = get_statusline_data(cc_json=cc_json)

        # Format and output
        statusline = format_statusline(
            agent_name=data["agent_name"],
            project=data["project"],
            environment=data["environment"],
            tokens_used=data["tokens_used"],
            tokens_total=data["tokens_total"],
            cl=data["cl"]
        )

        print(statusline)
        return 0

    except Exception as e:
        print(f"Error generating statusline: {e}", file=sys.stderr)
        return 1


def cmd_statusline_install(args: argparse.Namespace) -> int:
    """Install statusline script and configure Claude Code settings."""
    from pathlib import Path
    import stat

    try:
        # Find .claude directory (project or global)
        cwd = Path.cwd()
        claude_dir = cwd / ".claude"

        if not claude_dir.exists():
            # Try global directory
            claude_dir = Path.home() / ".claude"
            if not claude_dir.exists():
                print("Error: No .claude directory found (checked project and ~/.claude)", file=sys.stderr)
                return 1

        # Create statusline.sh wrapper script
        script_path = claude_dir / "statusline.sh"
        script_content = """#!/bin/bash
# MacEff Statusline for Claude Code
exec macf_tools statusline
"""

        script_path.write_text(script_content)

        # Make executable
        script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        # Update settings.local.json
        settings_path = claude_dir / "settings.local.json"

        # Read existing settings or create empty dict
        if settings_path.exists():
            settings = json.loads(settings_path.read_text())
        else:
            settings = {}

        # Add statusLine configuration
        settings["statusLine"] = {
            "type": "command",
            "command": ".claude/statusline.sh",
            "padding": 0
        }

        # Write back
        settings_path.write_text(json.dumps(settings, indent=2) + "\n")

        print(f"✅ Statusline installed successfully:")
        print(f"   Script: {script_path}")
        print(f"   Settings: {settings_path}")
        print(f"\nRestart Claude Code to see the statusline.")

        return 0

    except Exception as e:
        print(f"Error installing statusline: {e}", file=sys.stderr)
        return 1


def cmd_breadcrumb(args: argparse.Namespace) -> int:
    """Generate fresh breadcrumb for current DEV_DRV."""
    from .utils import get_breadcrumb, parse_breadcrumb

    try:
        # Use the canonical get_breadcrumb() utility (DRY - single source of truth)
        breadcrumb = get_breadcrumb()

        # Output format based on flags
        if getattr(args, 'json_output', False):
            # Parse breadcrumb to extract components
            components = parse_breadcrumb(breadcrumb) or {}
            output = {
                "breadcrumb": breadcrumb,
                "components": components
            }
            print(json.dumps(output, indent=2))
        else:
            # Simple string output (default)
            print(breadcrumb)

        return 0

    except Exception as e:
        print(f"🏗️ MACF | ❌ Breadcrumb error: {e}", file=sys.stderr)
        return 1


def cmd_dev_drv(args: argparse.Namespace) -> int:
    """Extract and display DEV_DRV from JSONL using breadcrumb."""
    from .forensics.dev_drive import extract_dev_drive, render_markdown_summary, render_raw_jsonl
    from .utils import parse_breadcrumb

    try:
        # Parse breadcrumb
        breadcrumb_data = parse_breadcrumb(args.breadcrumb)
        if not breadcrumb_data:
            print(f"Error: Invalid breadcrumb format: {args.breadcrumb}")
            print("Expected format: s_abc12345/c_42/g_abc1234/p_def5678/t_1234567890")
            return 1

        # Extract DEV_DRV from JSONL
        drive = extract_dev_drive(
            session_id=breadcrumb_data['session_id'],
            prompt_uuid=breadcrumb_data['prompt_uuid'],
            breadcrumb_data=breadcrumb_data
        )

        if not drive:
            print(f"Error: Could not extract DEV_DRV for breadcrumb: {args.breadcrumb}")
            print(f"Session: {breadcrumb_data['session_id']}")
            print(f"Prompt: {breadcrumb_data['prompt_uuid']}")
            return 1

        # Render output based on format flag
        if args.raw:
            output = render_raw_jsonl(drive)
        else:
            # Default: markdown
            output = render_markdown_summary(drive)

        # Write to file or stdout
        if args.output:
            output_path = Path(args.output)
            output_path.write_text(output)
            print(f"DEV_DRV written to: {output_path}")
        else:
            print(output)

        return 0

    except Exception as e:
        print(f"Error extracting DEV_DRV: {e}")
        import traceback
        traceback.print_exc()
        return 1


def cmd_backup_create(args: argparse.Namespace) -> int:
    """Create consciousness backup archive."""
    from .backup import get_backup_paths, collect_backup_sources, create_archive
    paths = get_backup_paths(output_dir=args.output)
    sources = collect_backup_sources(
        paths,
        include_transcripts=not args.no_transcripts,
        quick_mode=args.quick
    )
    archive_path = create_archive(sources, paths)
    print(f"Created: {archive_path}")
    return 0


def cmd_backup_list(args: argparse.Namespace) -> int:
    """List backup archives in directory."""
    from pathlib import Path
    import json
    scan_dir = args.dir or Path.cwd()
    archives = list(scan_dir.glob("*_consciousness.tar.xz"))
    if args.json_output:
        print(json.dumps([str(a) for a in archives], indent=2))
    else:
        for a in sorted(archives):
            print(a.name)
    return 0


def cmd_backup_info(args: argparse.Namespace) -> int:
    """Show backup archive info."""
    from .backup.archive import get_archive_manifest
    import json
    manifest = get_archive_manifest(args.archive)
    if manifest:
        if args.json_output:
            print(json.dumps(manifest, indent=2))
        else:
            print(f"Project: {manifest.get('project_name')}")
            print(f"Created: {manifest.get('created_at')}")
            print(f"Files: {manifest['totals']['file_count']}")
            print(f"Size: {manifest['totals']['total_bytes']} bytes")
    return 0


def cmd_restore_verify(args: argparse.Namespace) -> int:
    """Verify archive integrity."""
    from .backup.archive import get_archive_manifest, extract_archive
    from .backup.manifest import verify_manifest
    import tempfile
    manifest = get_archive_manifest(args.archive)
    if not manifest:
        print("No manifest found in archive")
        return 1
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_archive(args.archive, Path(tmpdir))
        result = verify_manifest(manifest, Path(tmpdir))

    broken_symlinks = result.get("broken_symlinks", [])
    has_errors = not result["valid"]
    has_symlink_warnings = len(broken_symlinks) > 0

    if not has_errors and not has_symlink_warnings:
        print(f"Archive valid: {result['checked']} files verified")
        return 0

    if has_errors:
        print(f"Archive INVALID: {len(result['corrupted'])} corrupted, {len(result['missing'])} missing")
    else:
        print(f"Archive valid: {result['checked']} files verified")

    # Report broken symlinks (warning, not error)
    if has_symlink_warnings:
        print(f"\n⚠️  {len(broken_symlinks)} broken symlinks (targets don't exist on this system)")
        print("   These are hooks/commands pointing to source system paths.")
        print("   Use --transplant with 'restore install' to rewrite paths for this system:")
        print("   macf_tools agent restore install <archive> --target <dir> --transplant")
        if hasattr(args, 'verbose') and args.verbose:
            print("\n   Broken symlinks:")
            for s in broken_symlinks:
                print(f"     {s['path']} -> {s['target']}")

    if hasattr(args, 'verbose') and args.verbose:
        if result['missing']:
            print("\nMissing files:")
            for f in result['missing']:
                print(f"  - {f}")
        if result['corrupted']:
            print("\nCorrupted files:")
            for f in result['corrupted']:
                print(f"  - {f['path']}: expected {f['expected'][:8]}... got {f['actual'][:8]}...")

    return 1 if has_errors else 0


def cmd_restore_install(args: argparse.Namespace) -> int:
    """Install backup to target directory with optional transplant."""
    from .backup.archive import extract_archive, get_archive_manifest, list_archive
    from .backup.integrity import (
        has_existing_consciousness,
        detect_existing_consciousness,
        create_recovery_checkpoint,
        format_safety_warning,
    )

    target = args.target or Path.cwd()

    # Safety check: detect existing consciousness
    if has_existing_consciousness(target) and not args.force:
        checks = detect_existing_consciousness(target)
        print(format_safety_warning(checks))
        return 1

    if args.dry_run:
        contents = list_archive(args.archive)
        print(f"Would extract {len(contents)} items to {target}")

        if has_existing_consciousness(target):
            print("\nWould create recovery checkpoint before overwriting")

        if args.transplant:
            manifest = get_archive_manifest(args.archive)
            if manifest:
                from .backup.transplant import create_transplant_mapping
                maceff_root = args.maceff_root or (target.parent / "MacEff")
                mapping = create_transplant_mapping(manifest, target, maceff_root)
                print(f"\nTransplant would rewrite paths:")
                print(f"  Project: {mapping.source_project_root} -> {mapping.target_project_root}")
                print(f"  MacEff:  {mapping.source_maceff_root} -> {mapping.target_maceff_root}")
                print(f"  Home:    {mapping.source_home} -> {mapping.target_home}")
        return 0

    # Create recovery checkpoint if overwriting existing consciousness
    if has_existing_consciousness(target):
        checkpoint = create_recovery_checkpoint(target)
        if checkpoint:
            print(f"Recovery checkpoint created: {checkpoint}")

    # Extract archive
    manifest = extract_archive(args.archive, target)
    print(f"Extracted to: {target}")

    # Run transplant if requested
    if args.transplant:
        from .backup.transplant import create_transplant_mapping, run_transplant, transplant_summary
        maceff_root = args.maceff_root or (target.parent / "MacEff")
        mapping = create_transplant_mapping(manifest, target, maceff_root)
        changes = run_transplant(target, mapping, dry_run=False)
        print(f"\n{transplant_summary(changes)}")

        # Suggest running hooks install
        print("\nNext step: Run 'macf_tools hooks install' to complete setup")

    return 0


def cmd_agent_init(args: argparse.Namespace) -> int:
    """Initialize agent with preamble injection (idempotent)."""
    try:
        # Detect PA home directory
        config = ConsciousnessConfig()
        if config._is_container():
            # In container: use detected home
            pa_home = Path.home()
        else:
            # On host: use agent home
            try:
                from .utils import find_agent_home
                agent_home = find_agent_home()
                if agent_home:
                    pa_home = agent_home
                else:
                    pa_home = Path.cwd()
            except Exception:
                pa_home = Path.cwd()

        claude_md_path = pa_home / "CLAUDE.md"

        # Determine preamble template path (portable)
        template_locations = []

        # 1. Environment variable (deployment-configurable)
        env_templates = os.getenv("MACEFF_TEMPLATES_DIR")
        if env_templates:
            template_locations.append(Path(env_templates) / "PA_PREAMBLE.md")

        # 2. MacEff installation root (via find_maceff_root - works in container and host)
        try:
            maceff_root = find_maceff_root()
            if maceff_root:
                template_locations.append(maceff_root / "framework" / "templates" / "PA_PREAMBLE.md")
        except Exception:
            pass

        # 3. Development mode (relative to current directory - fallback with warning)
        cwd_fallback = Path.cwd() / "templates" / "PA_PREAMBLE.md"
        template_locations.append(cwd_fallback)

        preamble_template_path = None
        for loc in template_locations:
            if loc.exists():
                preamble_template_path = loc
                break

        # Warn if using CWD fallback (likely unintended)
        if preamble_template_path == cwd_fallback:
            print(f"⚠️  Warning: Using CWD fallback for template: {cwd_fallback}", file=sys.stderr)
            print("   Consider setting MACEFF_TEMPLATES_DIR or MACEFF_ROOT_DIR", file=sys.stderr)

        if not preamble_template_path:
            print("Error: PA_PREAMBLE.md template not found")
            print("Expected locations:")
            for loc in template_locations:
                print(f"  - {loc}")
            return 1

        # Read preamble template
        preamble_content = preamble_template_path.read_text()

        # Upgrade boundary marker
        UPGRADE_BOUNDARY = """---

<!-- ⚠️ DO NOT WRITE BELOW THIS LINE ⚠️ -->
<!-- Framework preamble managed by macf_tools - edits below will be lost on upgrade -->
<!-- Add custom policies and agent-specific content ABOVE this boundary -->
"""

        # Check if CLAUDE.md exists and process accordingly
        if claude_md_path.exists():
            existing_content = claude_md_path.read_text()

            # If boundary exists, extract user content above it
            if "<!-- ⚠️ DO NOT WRITE BELOW THIS LINE" in existing_content:
                user_content = existing_content.split("<!-- ⚠️ DO NOT WRITE BELOW THIS LINE")[0].rstrip()
                action_desc = "Update PA Preamble in existing"
            else:
                # No boundary = first time, preserve all existing content
                user_content = existing_content.rstrip()
                action_desc = "⚠️  Add PA Preamble to existing"

            # Confirmation prompt for modifying existing file
            print(f"\n{action_desc} CLAUDE.md:")
            print(f"  📄 {claude_md_path}")
            if not getattr(args, 'yes', False):
                response = input("\nProceed? [y/N]: ").strip().lower()
                if response != 'y':
                    print("Aborted.")
                    return 0

            # Append: user + boundary + preamble
            new_content = user_content + "\n\n" + UPGRADE_BOUNDARY + "\n\n" + preamble_content
            claude_md_path.write_text(new_content)
            print(f"✅ PA Preamble appended successfully")
        else:
            # Create new CLAUDE.md with just the preamble (no boundary needed)
            print(f"\nCreate new CLAUDE.md with PA Preamble:")
            print(f"  📄 {claude_md_path}")
            if not getattr(args, 'yes', False):
                response = input("\nProceed? [y/N]: ").strip().lower()
                if response != 'y':
                    print("Aborted.")
                    return 0
            claude_md_path.write_text(preamble_content)
            print(f"✅ CLAUDE.md created successfully")

        # Create personal policy directory structure (PA only)
        personal_policies_dir = pa_home / "agent" / "policies" / "personal"
        personal_policies_dir.mkdir(parents=True, exist_ok=True)

        # Create personal manifest if it doesn't exist
        personal_manifest = personal_policies_dir / "manifest.json"
        if not personal_manifest.exists():
            manifest_data = {
                "version": "1.0.0",
                "description": f"{config.agent_name} Personal Policies",
                "extends": "/opt/maceff/policies/manifest.json",
                "personal_policies": []
            }
            with open(personal_manifest, 'w') as f:
                json.dump(manifest_data, f, indent=2)
            print(f"✅ Created personal policy manifest at {personal_manifest}")

        print(f"\n📍 PA Home: {pa_home}")
        print(f"📍 Personal Policies: {personal_policies_dir}")
        print(f"\nAgent initialization complete!")

        return 0

    except Exception as e:
        print(f"Error during agent initialization: {e}")
        return 1


# TODO: Migrate policy read caching to event-first architecture
# Legacy _get_policy_read_cache and _update_policy_read_cache deleted (used session_state.json)
# Implementation needed:
#   1. _get_policy_read_from_events(policy_name) - scan events backwards until session_started/compaction_detected
#      - Look for 'policy_read' events with matching policy_name
#      - Return breadcrumb if found, None otherwise
#   2. _record_policy_read_event(policy_name, breadcrumb) - append 'policy_read' event
# Call sites at lines ~1240 and ~1255 reference deleted functions - currently broken


def cmd_policy_navigate(args: argparse.Namespace) -> int:
    """Navigate policy by showing CEP guide only (up to CEP_NAV_BOUNDARY)."""
    from .utils import find_policy_file

    try:
        policy_name = args.policy_name
        # Parse optional parent from path-like input (e.g., "development/todo_hygiene")
        parents = None
        if '/' in policy_name:
            parts = policy_name.split('/')
            policy_name = parts[-1]
            parents = parts[:-1]

        policy_path = find_policy_file(policy_name, parents=parents)

        if not policy_path:
            print(f"Policy not found: {args.policy_name}")
            print("\nUse 'macf_tools policy list' to see available policies")
            return 1

        # Read file and extract content up to CEP_NAV_BOUNDARY
        content = policy_path.read_text()

        boundary_marker = "=== CEP_NAV_BOUNDARY ==="
        if boundary_marker in content:
            nav_content = content.split(boundary_marker)[0]
        else:
            # No boundary - show first 100 lines as navigation
            lines = content.split('\n')[:100]
            nav_content = '\n'.join(lines)
            nav_content += f"\n\n[No CEP_NAV_BOUNDARY found - showing first 100 lines]"

        # Output with line numbers
        print(f"=== CEP Navigation Guide: {policy_path.name} ===\n")
        nav_lines = nav_content.split('\n')
        for i, line in enumerate(nav_lines, 1):
            print(f"{i:4d}│ {line}")

        print(f"\n=== End Navigation Guide ===")

        # Discovery flow footer with guidance
        print(f"\nTo read full policy: macf_tools policy read {args.policy_name}")
        print(f"To read specific section: macf_tools policy read {args.policy_name} --section N (e.g., --section 5 or --section 5.1)")

        # Estimate tokens: ~4 tokens/line average for markdown, display in k
        full_lines = len(content.split('\n'))
        est_tokens_k = (full_lines * 4) / 1000
        print(f"\n📊 Full policy: ~{full_lines} lines (~{est_tokens_k:.1f}k tokens)")

        return 0

    except Exception as e:
        print(f"Error navigating policy: {e}")
        return 1


def cmd_policy_read(args: argparse.Namespace) -> int:
    """Read policy file with line numbers and optional caching."""
    from .utils import find_policy_file, get_breadcrumb

    try:
        policy_name = args.policy_name
        # Parse optional parent from path-like input
        parents = None
        if '/' in policy_name:
            parts = policy_name.split('/')
            policy_name = parts[-1]
            parents = parts[:-1]

        policy_path = find_policy_file(policy_name, parents=parents)

        if not policy_path:
            print(f"Policy not found: {args.policy_name}")
            print("\nUse 'macf_tools policy list' to see available policies")
            return 1

        # Read full content
        content = policy_path.read_text()
        lines = content.split('\n')

        # Get session for caching
        session_id = get_current_session_id()
        cache_key = policy_path.stem  # Use stem for cache key

        # Check if this is a partial read (--lines or --section or --from-nav-boundary)
        from_nav = hasattr(args, 'from_nav_boundary') and args.from_nav_boundary
        is_partial = (hasattr(args, 'lines') and args.lines) or (hasattr(args, 'section') and args.section) or from_nav
        force_read = hasattr(args, 'force') and args.force
        line_offset = 1

        # Handle --from-nav-boundary option (skip CEP navigation guide)
        if from_nav:
            boundary_marker = "=== CEP_NAV_BOUNDARY ==="
            boundary_idx = None
            for i, line in enumerate(lines):
                if boundary_marker in line:
                    boundary_idx = i
                    break
            if boundary_idx is not None:
                lines = lines[boundary_idx + 1:]  # Start after boundary
                line_offset = boundary_idx + 2  # +2 for 1-indexed and skip boundary line
            # If no boundary found, read full file (no-op)

        # Handle --lines option (e.g., "50:100")
        elif hasattr(args, 'lines') and args.lines:
            try:
                parts = args.lines.split(':')
                start = int(parts[0]) - 1  # Convert to 0-indexed
                end = int(parts[1]) if len(parts) > 1 else len(lines)
                lines = lines[start:end]
                line_offset = start + 1
            except (ValueError, IndexError):
                print(f"Invalid --lines format: {args.lines}")
                print("Expected format: START:END (e.g., 50:100)")
                return 1
        # Handle --section option
        elif hasattr(args, 'section') and args.section:
            section_num = str(args.section)

            def matches_section_prefix(heading_num: str, target: str) -> bool:
                """Check if heading_num matches target section (hierarchical).

                Examples:
                    matches_section_prefix("10", "10") → True (exact)
                    matches_section_prefix("10.1", "10") → True (subsection)
                    matches_section_prefix("10", "10.1") → False (parent doesn't match child request)
                    matches_section_prefix("100", "10") → False (not a subsection!)
                """
                if heading_num == target:
                    return True
                # Check if heading is a subsection: must start with "target."
                return heading_num.startswith(target + ".")

            # Find section by heading number, include subsections
            # Stop only at same-or-higher level heading (not subsections)
            in_section = False
            section_lines = []
            section_start = 0
            section_level = 0  # Track heading level (## = 2, ### = 3, etc.)
            in_code_block = False  # Track if we're inside a fenced code block

            for i, line in enumerate(lines):
                # Track code block boundaries (``` or ~~~)
                if line.startswith('```') or line.startswith('~~~'):
                    in_code_block = not in_code_block

                # Only process headings outside code blocks
                if line.startswith('#') and not in_code_block:
                    # Count heading level
                    level = len(line) - len(line.lstrip('#'))
                    heading_text = line.lstrip('#').strip()

                    if heading_text:
                        heading_num = heading_text.split()[0].rstrip('.')

                        if matches_section_prefix(heading_num, section_num):
                            # Found target section or subsection
                            if not in_section:
                                # First match - record the section level
                                in_section = True
                                section_start = i + 1
                                section_level = level
                            # Subsequent matches (subsections) don't reset level
                        elif in_section and level <= section_level:
                            # Same or higher level heading = new section, stop
                            break
                        # else: subsection (deeper level), keep capturing

                if in_section:
                    section_lines.append(line)

            if not section_lines:
                print(f"Section {section_num} not found in {policy_name}")
                return 1

            lines = section_lines
            line_offset = section_start
        else:
            # TODO: Re-enable event-first cache check when implemented
            # Full read - cache check disabled pending event-first migration
            pass

        # Output with line numbers
        print(f"=== {policy_path.name} ===\n")
        for i, line in enumerate(lines, line_offset):
            print(f"{i:4d}│ {line}")

        # TODO: Re-enable event-first cache recording when implemented
        # Cache recording disabled pending event-first migration
        if not is_partial:
            breadcrumb = get_breadcrumb()
            print(f"\n=== Read at {breadcrumb} (caching disabled) ===")
        else:
            print(f"\n=== Partial read (not cached) ===")

        # Show policy metadata footer
        import os
        from datetime import datetime
        mtime = os.path.getmtime(policy_path)
        last_modified = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
        print(f"\n📅 Last updated: {last_modified}")
        if is_partial:
            print(f"💡 Run `macf_tools policy navigate {args.policy_name}` to see all sections")

        return 0

    except Exception as e:
        print(f"Error reading policy: {e}")
        import traceback
        traceback.print_exc()
        return 1


def cmd_policy_manifest(args: argparse.Namespace) -> int:
    """Display merged and filtered policy manifest."""
    from .utils import load_merged_manifest, filter_active_policies

    try:
        # Load and filter manifest
        manifest = load_merged_manifest()
        filtered = filter_active_policies(manifest)

        # Choose format
        format_type = getattr(args, 'format', 'summary')

        if format_type == 'json':
            # Pretty-print full filtered manifest
            print(json.dumps(filtered, indent=2))
        else:
            # Summary format
            print("Policy Manifest Summary")
            print("=" * 50)
            print(f"Version: {filtered.get('version', 'unknown')}")
            print(f"Description: {filtered.get('description', 'N/A')}")

            # Active layers
            active_layers = manifest.get('active_layers', [])
            if active_layers:
                print(f"Active Layers: {', '.join(active_layers)}")
            else:
                print("Active Layers: none configured")

            # Active languages
            active_languages = manifest.get('active_languages', [])
            if active_languages:
                print(f"Active Languages: {', '.join(active_languages)}")
            else:
                print("Active Languages: none configured")

            # CA type count
            discovery_index = filtered.get('discovery_index', {})
            ca_types = set()
            for key in discovery_index.keys():
                # Extract CA types from discovery index keys
                if any(ca in key for ca in ['observation', 'experiment', 'report', 'reflection', 'checkpoint', 'roadmap', 'emotion']):
                    if 'observation' in key:
                        ca_types.add('observations')
                    if 'experiment' in key:
                        ca_types.add('experiments')
                    if 'report' in key:
                        ca_types.add('reports')
                    if 'reflection' in key:
                        ca_types.add('reflections')
                    if 'checkpoint' in key or 'ccp' in key:
                        ca_types.add('checkpoints')
                    if 'roadmap' in key:
                        ca_types.add('roadmaps')
                    if 'emotion' in key:
                        ca_types.add('emotions')

            print(f"CA Types Configured: {len(ca_types)}")
            if ca_types:
                print(f"  Types: {', '.join(sorted(ca_types))}")

        return 0

    except Exception as e:
        print(f"Error displaying manifest: {e}")
        return 1


def cmd_policy_search(args: argparse.Namespace) -> int:
    """Search for keyword in policy manifest with section-level results."""
    from .utils import load_merged_manifest, filter_active_policies

    try:
        keyword = args.keyword.lower()

        # Load and filter manifest
        manifest = load_merged_manifest()
        filtered = filter_active_policies(manifest)

        policy_matches = []  # (category, name, description)
        section_matches = []  # (index_key, policy_ref)

        def search_policy_dict(policy: dict, category: str) -> bool:
            """Check if a policy dict matches the keyword. Returns True if matched."""
            name = policy.get('name', '')
            desc = policy.get('description', '')
            keywords_list = policy.get('keywords', [])

            if (keyword in name.lower() or
                keyword in desc.lower() or
                any(keyword in kw.lower() for kw in keywords_list)):
                policy_matches.append((category, name, desc or name))
                return True
            return False

        def search_policies_recursive(data: any, category: str) -> None:
            """Recursively search for policies in any manifest structure."""
            if isinstance(data, dict):
                # Check if this dict has 'policies' key (standard policy list)
                if 'policies' in data and isinstance(data['policies'], list):
                    for policy in data['policies']:
                        if isinstance(policy, dict):
                            search_policy_dict(policy, category)
                # Check if this dict has 'triggers' key (consciousness_patterns)
                elif 'triggers' in data and isinstance(data['triggers'], list):
                    for trigger in data['triggers']:
                        if isinstance(trigger, dict):
                            pattern_name = trigger.get('pattern', '')
                            consciousness = trigger.get('consciousness', '')
                            search_terms = trigger.get('search_terms', [])
                            if (keyword in pattern_name.lower() or
                                keyword in consciousness.lower() or
                                any(keyword in term.lower() for term in search_terms)):
                                policy_matches.append(('pattern', pattern_name, consciousness))
                # Check if this dict looks like a policy itself (has 'name' and 'keywords')
                elif 'name' in data and 'keywords' in data:
                    search_policy_dict(data, category)
                # Otherwise recurse into nested structures
                else:
                    for key, value in data.items():
                        if key not in ('description', 'location', 'opt_in', 'version',
                                       'last_updated', 'base_path', 'discovery_index',
                                       'consciousness_artifacts'):
                            sub_category = f"{category}/{key}" if category else key
                            search_policies_recursive(value, sub_category)
            elif isinstance(data, list):
                for item in data:
                    search_policies_recursive(item, category)

        # Search all policy categories dynamically
        for key, value in filtered.items():
            if key.endswith('_policies') or key == 'consciousness_patterns':
                category = key.replace('_policies', '').replace('_', ' ')
                search_policies_recursive(value, category)

        # Search discovery_index for section-level matches
        discovery_index = filtered.get('discovery_index', {})
        for index_key, policy_refs in discovery_index.items():
            if keyword in index_key.lower():
                for ref in policy_refs:
                    section_matches.append((index_key, ref))

        # Display results
        total = len(policy_matches) + len(section_matches)
        print(f"Search results for '{keyword}': {total} matches")
        print("=" * 50)

        if policy_matches:
            print("\n📋 Policy Matches:")
            for category, name, desc in policy_matches:
                print(f"  [{category}] {name}: {desc}")

        if section_matches:
            print("\n📍 Section Matches (from discovery index):")
            for index_key, ref in section_matches:
                print(f"  [{index_key}] → {ref}")

        if not policy_matches and not section_matches:
            print("No matches found")
            print("\n💡 Try:")
            print("  macf_tools policy list              # Browse all policies")
            print("  macf_tools policy search <keyword>  # Try different keyword")
        else:
            # Guide toward discovery flow: search → navigate → read
            print("\n💡 Next steps:")
            print("  macf_tools policy navigate <name>          # See CEP navigation guide")
            print("  macf_tools policy read <name> --section N  # Read specific section")

        return 0

    except Exception as e:
        print(f"Error searching manifest: {e}")
        return 1


def cmd_policy_list(args: argparse.Namespace) -> int:
    """List policy files from framework with optional filtering."""
    from .utils import list_policy_files
    from .event_queries import get_active_policy_injections_from_events

    try:
        tier = getattr(args, 'tier', None)
        category = getattr(args, 'category', None)

        # Get active injections for 💉 marker
        active_injections = {inj["policy_name"] for inj in get_active_policy_injections_from_events()}

        # Always extract tier info for all policies
        policies = list_policy_files(tier=tier, category=category, include_tier=True)

        if tier or category:
            filter_desc = []
            if tier:
                filter_desc.append(f"tier={tier}")
            if category:
                filter_desc.append(f"category={category}")
            print(f"Policies ({', '.join(filter_desc)})")
        else:
            print("All Policies")
        print("=" * 50)

        if not policies:
            print("No policies found")
            return 0

        # Group by category for display
        by_category = {}
        core_count = 0
        for p in policies:
            cat = p['category']
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(p)
            tier_val = p.get('tier') or ''
            if tier_val.upper() == 'CORE':
                core_count += 1

        for cat in sorted(by_category.keys()):
            print(f"\n{cat}/")
            for p in by_category[cat]:
                policy_tier = (p.get('tier') or '').upper()
                if policy_tier == 'CORE':
                    tier_str = " [CORE]"
                elif policy_tier:
                    tier_str = f" [{policy_tier}]"
                else:
                    tier_str = ""
                inject_marker = "💉 " if p['name'] in active_injections else "  "
                print(f"{inject_marker}{p['name']}.md{tier_str}")

        # Summary with CORE highlight
        print(f"\nTotal: {len(policies)} policies ({core_count} CORE)")

        # Discovery footer - guide agents to next step
        print("\n" + "-" * 50)
        print("💡 Run `macf_tools policy navigate <name>` to explore any policy")
        return 0

    except Exception as e:
        print(f"Error listing policies: {e}")
        return 1


def cmd_policy_ca_types(args: argparse.Namespace) -> int:
    """Show CA types with emojis."""
    from .utils import load_merged_manifest, filter_active_policies

    try:
        # CA emoji mapping
        CA_EMOJIS = {
            'observations': '🔬',
            'experiments': '🧪',
            'reports': '📊',
            'reflections': '💭',
            'checkpoints': '🔖',
            'roadmaps': '🗺️',
            'emotions': '❤️'
        }

        # Load and filter manifest
        manifest = load_merged_manifest()
        filtered = filter_active_policies(manifest)

        # Detect active CA types from discovery_index
        discovery_index = filtered.get('discovery_index', {})
        active_types = set()

        for key in discovery_index.keys():
            # Map discovery keys to CA types
            if 'observation' in key:
                active_types.add('observations')
            if 'experiment' in key:
                active_types.add('experiments')
            if 'report' in key:
                active_types.add('reports')
            if 'reflection' in key or 'jotewr' in key or 'wisdom' in key:
                active_types.add('reflections')
            if 'checkpoint' in key or 'ccp' in key:
                active_types.add('checkpoints')
            if 'roadmap' in key:
                active_types.add('roadmaps')
            if 'emotion' in key:
                active_types.add('emotions')

        print("Consciousness Artifact (CA) Types")
        print("=" * 50)

        if active_types:
            for ca_type in sorted(active_types):
                emoji = CA_EMOJIS.get(ca_type, '📄')
                print(f"{emoji} {ca_type}")
        else:
            print("No CA types configured")

        return 0

    except Exception as e:
        print(f"Error showing CA types: {e}")
        return 1


def cmd_policy_recommend(args: argparse.Namespace) -> int:
    """Get hybrid search policy recommendations using RRF scoring.

    First tries the warm search service (fast, ~20ms), falls back to
    direct search (slow, ~8s) if service unavailable.
    """
    import sys
    from .search_service.client import query_search_service

    query = args.query
    json_output = getattr(args, 'json_output', False)
    explain = getattr(args, 'explain', False)
    limit = getattr(args, 'limit', 5)

    if len(query) < 10:
        print("⚠️ Query too short (minimum 10 characters)")
        return 1

    # Try warm service first (fast path)
    result = query_search_service("policy", query, limit=limit, timeout_s=1.0)

    if result.get("formatted") and not result.get("error"):
        # Service responded - use fast path
        formatted = result["formatted"]
        explanations = result.get("explanations", [])
    else:
        # Service unavailable - fall back to direct search (slow)
        print("⚠️ Search service unavailable, using direct search (~8s)...",
              file=sys.stderr)
        print("   Start service: macf_tools search-service start", file=sys.stderr)
        try:
            from .utils.recommend import get_recommendations
            formatted, explanations = get_recommendations(query)
        except ImportError as e:
            print("⚠️ Policy recommend requires optional dependencies:")
            print("   pip install lancedb sentence-transformers")
            print(f"\nImport error: {e}")
            return 1

    try:

        if not formatted and not explanations:
            if json_output:
                import json
                print(json.dumps({"results": [], "query": query}))
            else:
                print("No recommendations found for query.")
                print("\n💡 Tips:")
                print("  - Try more specific keywords")
                print("  - Use policy-related terms (TODO, backup, checkpoint, etc.)")
            return 0

        # Limit results
        explanations = explanations[:limit]

        if json_output:
            import json
            output = {
                "results": explanations,
                "query": query,
                "engine": "rrf_hybrid",
            }
            print(json.dumps(output, indent=2))
        elif explain:
            # Use library function for verbose output
            print(format_verbose_output(explanations, query))
        else:
            # Default: rich human output from library
            print(formatted)

        return 0

    except Exception as e:
        if json_output:
            import json
            print(json.dumps({"error": str(e), "query": query}))
        else:
            print(f"❌ Error getting recommendations: {e}")
        return 1


def cmd_policy_build_index(args: argparse.Namespace) -> int:
    """Build hybrid FTS5 + semantic index from policy files."""
    try:
        from .hybrid_search import PolicyIndexer
    except ImportError as e:
        print("⚠️ Policy build_index requires optional dependencies:")
        print("   pip install sqlite-vec sentence-transformers")
        print(f"\nImport error: {e}")
        return 1

    from pathlib import Path
    from .utils.recommend import get_policy_db_path
    from .utils.manifest import get_framework_policies_path

    # Get paths with defaults
    policies_dir = Path(args.policies_dir) if args.policies_dir else get_framework_policies_path()
    if policies_dir is None:
        print("❌ Could not locate framework policies directory")
        print("   Use --policies-dir to specify manually")
        return 1

    db_path = Path(args.db_path) if args.db_path else get_policy_db_path()
    json_output = getattr(args, 'json_output', False)

    try:
        # Build index
        manifest_path = policies_dir / "manifest.json"
        indexer = PolicyIndexer(manifest_path=manifest_path if manifest_path.exists() else None)
        stats = indexer.build_index(
            policies_dir=policies_dir,
            db_path=db_path,
        )

        # Output
        if json_output:
            import json
            print(json.dumps(stats, indent=2))
        else:
            print("✅ Policy index built:")
            print(f"   Documents: {stats.get('documents_indexed', 0)}")
            print(f"   Questions: {stats.get('questions_indexed', 0)}")
            print(f"   Total time: {stats.get('total_time', 0):.2f}s")
            print(f"   Database: {db_path}")

        return 0

    except Exception as e:
        if json_output:
            import json
            print(json.dumps({"error": str(e)}))
        else:
            print(f"❌ Error building index: {e}")
        return 1


# -------- Policy Injection Commands --------

def cmd_policy_inject(args: argparse.Namespace) -> int:
    """Activate policy injection into PreToolUse hooks."""
    from .utils import find_policy_file
    from .agent_events_log import append_event
    from .event_queries import get_active_policy_injections_from_events

    try:
        policy_name = args.policy_name
        # Parse optional parent from path-like input
        parents = None
        if '/' in policy_name:
            parts = policy_name.split('/')
            policy_name = parts[-1]
            parents = parts[:-1]

        policy_path = find_policy_file(policy_name, parents=parents)

        if not policy_path:
            print(f"❌ Policy not found: {args.policy_name}")
            print("\nUse 'macf_tools policy list' to see available policies")
            return 1

        # Emit activation event
        append_event("policy_injection_activated", {
            "policy_name": policy_name,
            "policy_path": str(policy_path)
        })

        # Show confirmation with active list
        active = get_active_policy_injections_from_events()
        active_names = [inj["policy_name"] for inj in active]

        print(f"✅ Injecting: {policy_name}.md")
        print(f"   Active injections: {active_names}")
        print("   Content will appear in PreToolUse hooks")
        return 0

    except Exception as e:
        print(f"❌ Error injecting policy: {e}")
        return 1


def cmd_policy_clear_injection(args: argparse.Namespace) -> int:
    """Clear a specific policy injection."""
    from .agent_events_log import append_event
    from .event_queries import get_active_policy_injections_from_events

    try:
        policy_name = args.policy_name
        # Strip path prefix if provided
        if '/' in policy_name:
            policy_name = policy_name.split('/')[-1]

        # Check if currently active
        active = get_active_policy_injections_from_events()
        active_names = [inj["policy_name"] for inj in active]

        if policy_name not in active_names:
            print(f"⚠️ Policy '{policy_name}' is not currently injected")
            if active_names:
                print(f"   Active injections: {active_names}")
            else:
                print("   No active injections")
            return 0

        # Emit clear event
        append_event("policy_injection_cleared", {
            "policy_name": policy_name
        })

        # Show remaining
        remaining = get_active_policy_injections_from_events()
        remaining_names = [inj["policy_name"] for inj in remaining]

        print(f"✅ Cleared injection: {policy_name}.md")
        if remaining_names:
            print(f"   Remaining: {remaining_names}")
        else:
            print("   No remaining injections")
        return 0

    except Exception as e:
        print(f"❌ Error clearing injection: {e}")
        return 1


def cmd_policy_clear_injections(args: argparse.Namespace) -> int:
    """Clear all policy injections."""
    from .agent_events_log import append_event
    from .event_queries import get_active_policy_injections_from_events

    try:
        # Get current count
        active = get_active_policy_injections_from_events()
        count = len(active)

        if count == 0:
            print("✅ No active injections to clear")
            return 0

        # Emit clear-all event
        append_event("policy_injections_cleared_all", {})

        print(f"✅ Cleared all policy injections (was {count} active)")
        return 0

    except Exception as e:
        print(f"❌ Error clearing injections: {e}")
        return 1


def cmd_policy_injections(args: argparse.Namespace) -> int:
    """List active policy injections."""
    from .event_queries import get_active_policy_injections_from_events

    try:
        active = get_active_policy_injections_from_events()

        if not active:
            print("No active policy injections")
            print("\nUse 'macf_tools policy inject <name>' to activate")
            return 0

        print("Active policy injections:")
        for inj in active:
            print(f"  💉 {inj['policy_name']} ({inj['policy_path']})")

        print(f"\nTotal: {len(active)} active")
        return 0

    except Exception as e:
        print(f"❌ Error listing injections: {e}")
        return 1


# -------- Mode Commands --------

def cmd_mode_get(args: argparse.Namespace) -> int:
    """Get current operating mode."""
    from .utils.cycles import detect_auto_mode

    try:
        session_id = get_current_session_id()
        enabled, source = detect_auto_mode(session_id)

        mode = "AUTO_MODE" if enabled else "MANUAL_MODE"

        if getattr(args, 'json_output', False):
            data = {
                "mode": mode,
                "enabled": enabled,
                "source": source,
                "session_id": session_id
            }
            print(json.dumps(data, indent=2))
        else:
            print(f"Mode: {mode}")
            print(f"Source: {source}")

        return 0

    except Exception as e:
        print(f"Error getting mode: {e}")
        return 1


def cmd_mode_set(args: argparse.Namespace) -> int:
    """Set operating mode (requires auth token for AUTO_MODE)."""
    from .utils.cycles import set_auto_mode

    try:
        mode = args.mode.upper()
        auth_token = getattr(args, 'auth_token', None)

        # Validate mode argument
        if mode not in ('AUTO_MODE', 'MANUAL_MODE'):
            print(f"Invalid mode: {mode}")
            print("Valid modes: AUTO_MODE, MANUAL_MODE")
            return 1

        enabled = (mode == 'AUTO_MODE')
        session_id = get_current_session_id()

        # AUTO_MODE requires auth token
        if enabled and not auth_token:
            print("Error: AUTO_MODE requires --auth-token")
            print("\nTo activate AUTO_MODE:")
            print("  macf_tools mode set AUTO_MODE --auth-token \"$(python3 -c \"import json; print(json.load(open('.maceff/settings.json'))['auto_mode_auth_token'])\")\"\n")
            return 1

        # Set mode
        success, message = set_auto_mode(
            enabled=enabled,
            session_id=session_id,
            auth_token=auth_token,
        )

        if success:
            print(f"✅ {message}")

            # If enabling AUTO_MODE, also enable autocompact and bypass permissions
            if enabled:
                from .utils.claude_settings import set_autocompact_enabled, set_permission_mode
                if set_autocompact_enabled(True):
                    print("✅ autoCompactEnabled set to true in ~/.claude.json")
                else:
                    print("⚠️  Could not update autoCompactEnabled setting")
                if set_permission_mode("bypassPermissions"):
                    print("✅ permissions.defaultMode set to bypassPermissions")
                else:
                    print("⚠️  Could not update permissions.defaultMode setting")
            else:
                # Returning to MANUAL_MODE - restore default permissions
                from .utils.claude_settings import set_autocompact_enabled, set_permission_mode
                set_autocompact_enabled(False)
                set_permission_mode("default")
                print("✅ Restored default settings (autocompact disabled, default permissions)")
        else:
            print(f"❌ {message}")
            return 1

        return 0

    except Exception as e:
        print(f"Error setting mode: {e}")
        return 1


# -------- Agent Events Log Commands --------

def cmd_events_show(args: argparse.Namespace) -> int:
    """Display current agent state from events log."""
    from .agent_events_log import get_current_state

    try:
        state = get_current_state()

        if getattr(args, 'json_output', False):
            # JSON output
            print(json.dumps(state, indent=2))
        else:
            # Human-readable output
            print("Current Agent State")
            print("=" * 50)
            print(f"Session ID: {state.get('session_id', 'N/A')}")
            print(f"Cycle: {state.get('cycle', 'N/A')}")

        return 0

    except Exception as e:
        print(f"Error reading current state: {e}")
        return 1


def cmd_events_history(args: argparse.Namespace) -> int:
    """Display recent events from log."""
    from .agent_events_log import read_events

    try:
        limit = getattr(args, 'limit', 10)

        print(f"Recent Events (last {limit})")
        print("=" * 50)

        events = list(read_events(limit=limit, reverse=True))

        if not events:
            print("No events found")
            return 0

        for event in events:
            timestamp = event.get('timestamp', 0)
            event_type = event.get('event', 'unknown')
            breadcrumb = event.get('breadcrumb', 'N/A')

            # Format timestamp
            dt = datetime.fromtimestamp(timestamp, tz=_pick_tz())
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")

            print(f"[{time_str}] {event_type}")
            print(f"  Breadcrumb: {breadcrumb}")

            # Show key data fields
            data = event.get('data', {})
            if data:
                for key, value in data.items():
                    print(f"  {key}: {value}")
            print()

        return 0

    except Exception as e:
        print(f"Error reading event history: {e}")
        return 1


def cmd_events_query(args: argparse.Namespace) -> int:
    """Query events with filters."""
    from .agent_events_log import query_events

    try:
        # Build filter dict from args
        filters = {}

        # Event type filter
        if hasattr(args, 'event') and args.event:
            filters['event_type'] = args.event

        # Breadcrumb filters
        breadcrumb_filters = {}

        if hasattr(args, 'cycle') and args.cycle:
            breadcrumb_filters['c'] = int(args.cycle)

        if hasattr(args, 'git_hash') and args.git_hash:
            breadcrumb_filters['g'] = args.git_hash

        if hasattr(args, 'session') and args.session:
            breadcrumb_filters['s'] = args.session

        if hasattr(args, 'prompt') and args.prompt:
            breadcrumb_filters['p'] = args.prompt

        if breadcrumb_filters:
            filters['breadcrumb'] = breadcrumb_filters

        # Timestamp filters
        if hasattr(args, 'after') and args.after:
            filters['since'] = float(args.after)

        if hasattr(args, 'before') and args.before:
            filters['until'] = float(args.before)

        # Execute query
        results = query_events(filters)

        # Post-filter by command if specified (for cli_command_invoked events)
        command_filter = getattr(args, 'command', None)
        if command_filter:
            filtered = []
            for event in results:
                if event.get('event') == 'cli_command_invoked':
                    argv = event.get('data', {}).get('argv', [])
                    cmd_str = ' '.join(argv)
                    if command_filter in cmd_str:
                        filtered.append(event)
            results = filtered

        print(f"Query Results: {len(results)} events")
        print("=" * 50)

        if not results:
            print("No matching events found")
            return 0

        verbose = getattr(args, 'verbose', False)

        for event in results:
            timestamp = event.get('timestamp', 0)
            event_type = event.get('event', 'unknown')
            breadcrumb = event.get('breadcrumb', 'N/A')
            data = event.get('data', {})

            # Format timestamp
            dt = datetime.fromtimestamp(timestamp, tz=_pick_tz())
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")

            print(f"[{time_str}] {event_type}")
            print(f"  Breadcrumb: {breadcrumb}")

            # Show command details for cli_command_invoked
            if event_type == 'cli_command_invoked' and 'argv' in data:
                argv = data.get('argv', [])
                print(f"  Command: {' '.join(argv)}")

            # Verbose mode: show all data
            if verbose and data:
                import json
                print(f"  Data: {json.dumps(data, indent=4)}")

            print()

        return 0

    except Exception as e:
        print(f"Error querying events: {e}")
        return 1


def cmd_events_query_set(args: argparse.Namespace) -> int:
    """Perform set operations on queries."""
    from .agent_events_log import query_events

    try:
        # Parse query and subtract arguments
        query_filters = {}
        subtract_filters = {}

        # Parse --query argument
        if hasattr(args, 'query') and args.query:
            # Format: "event_type=migration_detected" or "cycle=171"
            query_str = args.query
            if '=' in query_str:
                key, value = query_str.split('=', 1)
                if key == 'event_type':
                    query_filters['event_type'] = value
                elif key == 'cycle':
                    query_filters['breadcrumb'] = {'c': int(value)}

        # Parse --subtract argument
        if hasattr(args, 'subtract') and args.subtract:
            subtract_str = args.subtract
            if '=' in subtract_str:
                key, value = subtract_str.split('=', 1)
                if key == 'cycle':
                    subtract_filters['breadcrumb'] = {'c': int(value)}

        # Execute queries
        from .agent_events_log import query_set_operations

        queries = [query_filters, subtract_filters]
        results = query_set_operations(queries, 'subtraction')

        print(f"Set Operation Results: {len(results)} events")
        print("=" * 50)

        if not results:
            print("No events after set operation")
            return 0

        for event in results:
            timestamp = event.get('timestamp', 0)
            event_type = event.get('event', 'unknown')
            breadcrumb = event.get('breadcrumb', 'N/A')

            # Format timestamp
            dt = datetime.fromtimestamp(timestamp, tz=_pick_tz())
            time_str = dt.strftime("%Y-%m-%d %H:%M:%S")

            print(f"[{time_str}] {event_type}")
            print(f"  Breadcrumb: {breadcrumb}")
            print()

        return 0

    except Exception as e:
        print(f"Error performing set operation: {e}")
        return 1


def cmd_events_sessions_list(args: argparse.Namespace) -> int:
    """List all sessions from events log."""
    from .agent_events_log import read_events

    try:
        # Collect unique sessions
        sessions = {}

        for event in read_events(limit=None, reverse=False):
            data = event.get('data', {})
            session_id = data.get('session_id')

            if session_id:
                # Track session info
                if session_id not in sessions:
                    sessions[session_id] = {
                        'first_seen': event.get('timestamp', 0),
                        'last_seen': event.get('timestamp', 0),
                        'events': 1
                    }
                else:
                    sessions[session_id]['last_seen'] = event.get('timestamp', 0)
                    sessions[session_id]['events'] += 1

        print(f"Sessions: {len(sessions)} total")
        print("=" * 50)

        for session_id, info in sessions.items():
            # Show first 8 chars of session ID
            short_id = session_id[:8] if len(session_id) > 8 else session_id
            event_count = info['events']
            print(f"{short_id}... ({event_count} events)")

        return 0

    except Exception as e:
        print(f"Error listing sessions: {e}")
        return 1


def cmd_events_stats(args: argparse.Namespace) -> int:
    """Display event statistics."""
    from .agent_events_log import read_events

    try:
        # Count events by type
        event_counts = {}

        for event in read_events(limit=None, reverse=False):
            event_type = event.get('event', 'unknown')
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

        print("Event Statistics")
        print("=" * 50)

        if not event_counts:
            print("No events found")
            return 0

        for event_type, count in sorted(event_counts.items()):
            print(f"{event_type}: {count}")

        return 0

    except Exception as e:
        print(f"Error calculating statistics: {e}")
        return 1


def cmd_events_gaps(args: argparse.Namespace) -> int:
    """Detect time gaps between events (potential crashes)."""
    from .agent_events_log import read_events

    try:
        threshold = getattr(args, 'threshold', 3600)  # Default 1 hour
        threshold = float(threshold)

        print(f"Time Gap Analysis (threshold: {threshold}s)")
        print("=" * 50)

        events = list(read_events(limit=None, reverse=False))

        if len(events) < 2:
            print("Not enough events for gap analysis")
            return 0

        gaps_found = 0

        for i in range(1, len(events)):
            prev_event = events[i - 1]
            curr_event = events[i]

            prev_time = prev_event.get('timestamp', 0)
            curr_time = curr_event.get('timestamp', 0)

            gap = curr_time - prev_time

            if gap > threshold:
                gaps_found += 1

                # Format timestamps
                prev_dt = datetime.fromtimestamp(prev_time, tz=_pick_tz())
                curr_dt = datetime.fromtimestamp(curr_time, tz=_pick_tz())

                print(f"Gap #{gaps_found}: {gap:.0f}s")
                print(f"  From: {prev_dt.strftime('%Y-%m-%d %H:%M:%S')} ({prev_event.get('event')})")
                print(f"  To:   {curr_dt.strftime('%Y-%m-%d %H:%M:%S')} ({curr_event.get('event')})")
                print()

        if gaps_found == 0:
            print("No significant gaps detected")

        return 0

    except Exception as e:
        print(f"Error analyzing gaps: {e}")
        return 1


def cmd_task_list(args: argparse.Namespace) -> int:
    """List tasks from current session with hierarchy and metadata."""
    from .task import TaskReader, MacfTask

    reader = TaskReader()
    tasks = reader.read_all_tasks()

    if not tasks:
        print("No tasks found in current session.")
        return 0

    # Sort tasks by ID (string-safe with zero-padding)
    tasks = sorted(tasks, key=lambda t: str(t.id).zfill(10))

    # Apply archive visibility filters (before other filters)
    # By default, hide archived tasks unless --all or --archived is specified
    if args.show_archived_only:
        tasks = [t for t in tasks if t.status == "archived"]
    elif not args.show_all:
        tasks = [t for t in tasks if t.status != "archived"]

    # Apply filters
    if args.type_filter:
        type_upper = args.type_filter.upper()
        tasks = [t for t in tasks if t.task_type == type_upper]

    if args.status_filter:
        tasks = [t for t in tasks if t.status == args.status_filter]

    if args.parent_filter is not None:
        tasks = [t for t in tasks if t.parent_id == args.parent_filter]

    if not tasks:
        print("No tasks match filters.")
        return 0

    # JSON output
    if args.json_output:
        import json
        output = []
        for t in sorted(tasks, key=lambda t: str(t.id).zfill(10)):
            item = {
                "id": t.id,
                "subject": t.subject,
                "status": t.status,
                "type": t.task_type,
                "parent_id": t.parent_id,
                "blocked_by": t.blocked_by,
            }
            if t.mtmd:
                item["mtmd"] = {
                    "plan_ca_ref": t.mtmd.plan_ca_ref,
                    "creation_breadcrumb": t.mtmd.creation_breadcrumb,
                    "created_cycle": t.mtmd.created_cycle,
                    "repo": t.mtmd.repo,
                    "target_version": t.mtmd.target_version,
                }
            output.append(item)
        print(json.dumps(output, indent=2))
        return 0

    # Build hierarchy for tree display
    task_map = {t.id: t for t in tasks}
    root_tasks = [t for t in tasks if t.parent_id is None or t.parent_id not in task_map]
    # Sort root tasks numerically (zero-pad string IDs for proper ordering)
    root_tasks = sorted(root_tasks, key=lambda t: str(t.id).zfill(10))

    def get_children(parent_id):
        return sorted([t for t in tasks if t.parent_id == parent_id], key=lambda t: str(t.id).zfill(10))

    def format_task(t: MacfTask, indent: int = 0) -> str:
        prefix = "  " * indent
        # CC-style markers with colors:
        # ◼ red = in_progress, ◻ = pending, ✔ green = completed, ▫ = archived
        # Formatting: completed = strikethrough, archived = dim+strikethrough
        if t.status == "archived":
            # Cardboard brown filled box for archived (▪ with tan/brown color)
            ANSI_BROWN = "\033[38;5;137m"  # Tan/cardboard brown
            status_icon = f"{ANSI_BROWN}▪{ANSI_RESET}"
            # Dim + strikethrough for archived (strip embedded ANSI first)
            clean_subject = _strip_ansi(t.subject)
            line = f"{prefix}{status_icon} {ANSI_DIM}{ANSI_STRIKE}{clean_subject}{ANSI_RESET}"
        elif t.status == "completed":
            status_icon = f"{ANSI_GREEN}✔{ANSI_RESET}"
            # Strikethrough only for completed (strip embedded ANSI first)
            clean_subject = _strip_ansi(t.subject)
            line = f"{prefix}{status_icon} {ANSI_STRIKE}{clean_subject}{ANSI_RESET}"
        elif t.status == "in_progress":
            status_icon = f"{ANSI_RED}◼{ANSI_RESET}"
            line = f"{prefix}{status_icon} {_dim_task_ids(t.subject)}"
        else:  # pending
            status_icon = "◻"
            line = f"{prefix}{status_icon} {_dim_task_ids(t.subject)}"

        # Add plan_ca_ref if present (key feature of enhanced display)
        if t.mtmd and t.mtmd.plan_ca_ref:
            if t.status == "archived":
                line += f"\n{prefix}   {ANSI_DIM}{ANSI_STRIKE}→ {t.mtmd.plan_ca_ref}{ANSI_RESET}"
            elif t.status == "completed":
                line += f"\n{prefix}   {ANSI_STRIKE}→ {t.mtmd.plan_ca_ref}{ANSI_RESET}"
            else:
                line += f"\n{prefix}   → {t.mtmd.plan_ca_ref}"

        return line

    def print_tree(task: MacfTask, indent: int = 0):
        print(format_task(task, indent))
        for child in get_children(task.id):
            print_tree(child, indent + 1)

    # Print header
    print(f"📋 Tasks ({len(tasks)} total) - Session: {reader.session_uuid[:8]}...")
    print("-" * 60)

    # Print tree from roots (sorted by ID with zero-padding for numeric order)
    for root in sorted(root_tasks, key=lambda t: str(t.id).zfill(10)):
        print_tree(root)

    return 0


def cmd_task_get(args: argparse.Namespace) -> int:
    """Get detailed information about a specific task."""
    from .task import TaskReader

    # Parse task ID (handle #N or N format, support string IDs like "000")
    task_id_str = args.task_id.lstrip('#')
    # Keep as string if it has leading zeros (like "000"), otherwise try int
    if task_id_str.startswith('0') and len(task_id_str) > 1:
        task_id = task_id_str  # Preserve leading zeros (e.g., "000")
    else:
        try:
            task_id = int(task_id_str)
        except ValueError:
            task_id = task_id_str  # Use string directly for non-numeric IDs

    reader = TaskReader()
    task = reader.read_task(task_id)

    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    # JSON output
    if args.json_output:
        import json
        output = {
            "id": task.id,
            "subject": task.subject,
            "description": task.description,
            "status": task.status,
            "type": task.task_type,
            "parent_id": task.parent_id,
            "blocks": task.blocks,
            "blocked_by": task.blocked_by,
            "session_uuid": task.session_uuid,
            "file_path": task.file_path,
        }
        if task.mtmd:
            output["mtmd"] = {
                "version": task.mtmd.version,
                "creation_breadcrumb": task.mtmd.creation_breadcrumb,
                "created_cycle": task.mtmd.created_cycle,
                "created_by": task.mtmd.created_by,
                "plan_ca_ref": task.mtmd.plan_ca_ref,
                "experiment_ca_ref": task.mtmd.experiment_ca_ref,
                "parent_id": task.mtmd.parent_id,
                "repo": task.mtmd.repo,
                "target_version": task.mtmd.target_version,
                "release_branch": task.mtmd.release_branch,
                "completion_breadcrumb": task.mtmd.completion_breadcrumb,
                "completion_report": task.mtmd.completion_report,
                "unblock_breadcrumb": task.mtmd.unblock_breadcrumb,
                "updates": [u.to_dict() for u in task.mtmd.updates],
                "archived": task.mtmd.archived,
            }
        print(json.dumps(output, indent=2))
        return 0

    # Human-readable output
    status_icon = {"completed": "✅", "in_progress": "🔄", "pending": "⏳"}.get(task.status, "❓")

    print(f"{'='*60}")
    print(f"Task #{task.id} {status_icon}")
    print(f"{'='*60}")
    print(f"Subject: {task.subject}")
    print(f"Status: {task.status}")
    if task.task_type:
        print(f"Type: {task.task_type}")
    if task.parent_id:
        print(f"Parent: #{task.parent_id}")
    if task.blocked_by:
        print(f"Blocked by: {', '.join(f'#{b}' for b in task.blocked_by)}")
    if task.blocks:
        print(f"Blocks: {', '.join(f'#{b}' for b in task.blocks)}")

    # MTMD section - iterate dataclass fields in definition order
    if task.mtmd:
        from dataclasses import fields
        print(f"\nℹ️ MacfTaskMetaData (v{task.mtmd.version})")
        print("-" * 40)
        for f in fields(task.mtmd):
            if f.name == "version":
                continue  # Already shown in header
            value = getattr(task.mtmd, f.name)
            # Skip None/empty/False values
            if value is None or value == [] or value is False or value == {}:
                continue
            # Special handling for updates list
            if f.name == "updates" and value:
                print(f"  {f.name}: ({len(value)})")
                for u in value:
                    desc = f" - {u.description}" if u.description else ""
                    marker = "📝" if getattr(u, 'type', None) == "note" else "•"
                    print(f"    {marker} {u.breadcrumb}{desc}")
            else:
                print(f"  {f.name}: {value}")

    # Description (without MTMD)
    desc_clean = task.description_without_mtmd()
    if desc_clean:
        print(f"\n📝 Description")
        print("-" * 40)
        print(desc_clean)

    print(f"\n📁 File: {task.file_path}")

    return 0


def cmd_task_tree(args: argparse.Namespace) -> int:
    """Display task hierarchy tree from a root task."""
    import time
    from pathlib import Path
    from .task import TaskReader

    succinct = getattr(args, 'succinct', False)
    verbose = getattr(args, 'verbose', False)
    show_archived_only = getattr(args, 'archived', False)
    show_all = getattr(args, 'show_all', False)

    def display_tree(root_id: str):
        """Display the task tree for given root_id."""
        reader = TaskReader()
        all_tasks = reader.read_all_tasks()

        # Archive filtering (default: hide archived)
        def is_archived(task):
            return task.mtmd and getattr(task.mtmd, 'archived', False)

        if show_archived_only:
            all_tasks = [t for t in all_tasks if is_archived(t)]
        elif not show_all:
            all_tasks = [t for t in all_tasks if not is_archived(t)]
        # else show_all: no filtering

        task_map = {t.id: t for t in all_tasks}

        if root_id not in task_map:
            print(f"❌ Task #{root_id} not found")
            print(f"   Session: {reader.session_uuid}")
            print(f"   Tasks loaded: {len(all_tasks)}")
            if all_tasks:
                ids = sorted(t.id for t in all_tasks)
                print(f"   Available IDs: {ids[:10]}{'...' if len(ids) > 10 else ''}")
            return False

        root = task_map[root_id]

        def get_children(parent_id):
            # Zero-pad IDs for proper numeric string sorting
            return sorted([t for t in all_tasks if t.parent_id == parent_id], key=lambda t: t.id.zfill(10))

        def has_active_sibling(task, siblings):
            """Check if any sibling is active (in_progress or pending)."""
            for s in siblings:
                if s.id != task.id and s.status in ("in_progress", "pending"):
                    return True
            return False

        def should_show_task(task, siblings, depth):
            """Determine if task should be shown in succinct mode."""
            if not succinct:
                return True
            # Always show root sentinel (depth 0)
            if depth == 0:
                return True
            # Show if active/pending
            if task.status in ("in_progress", "pending"):
                return True
            # Top tier (depth 1): hide ALL completed - too many to show siblings
            if depth == 1:
                return False
            # Deeper tiers: show completed only if has active sibling (provides context)
            if task.status == "completed" and has_active_sibling(task, siblings):
                return True
            return False

        def count_descendants(task_id):
            children = get_children(task_id)
            return len(children) + sum(count_descendants(c.id) for c in children)

        def get_task_notes(task):
            """Extract notes from task MTMD updates."""
            if not task.mtmd or not task.mtmd.updates:
                return []
            return [u for u in task.mtmd.updates if getattr(u, 'type', None) == 'note']

        def get_task_plan(task):
            """Get plan or plan_ca_ref from task MTMD."""
            if not task.mtmd:
                return None, None
            return task.mtmd.plan, task.mtmd.plan_ca_ref

        def truncate(text, max_len=70):
            """Truncate text to max_len with ellipsis."""
            if not text:
                return ""
            text = text.replace('\n', ' ').strip()
            if len(text) <= max_len:
                return text
            return text[:max_len-3] + "..."

        def get_last_update_timestamp(task):
            """Extract Unix timestamp from last update's breadcrumb t_ field."""
            if not task.mtmd or not task.mtmd.updates:
                # Fall back to creation_breadcrumb if no updates
                bc = task.mtmd.creation_breadcrumb if task.mtmd else None
            else:
                # Get last update's breadcrumb
                bc = task.mtmd.updates[-1].breadcrumb
            if not bc:
                return None
            # Extract t_ timestamp from breadcrumb (format: s_.../c_.../g_.../p_.../t_XXXXXXXX)
            import re
            match = re.search(r't_(\d+)', bc)
            return int(match.group(1)) if match else None

        def format_task_suffix(task):
            """Format suffix: [repo version] timestamp with status-colored timestamp."""
            from datetime import datetime
            parts = []
            # Repo and version
            if task.mtmd:
                repo = task.mtmd.repo
                version = task.mtmd.target_version
                if repo or version:
                    rv = " ".join(filter(None, [repo, version]))
                    parts.append(f"[{rv}]")
            # Timestamp from last update
            ts = get_last_update_timestamp(task)
            if ts:
                dt = datetime.fromtimestamp(ts)
                time_str = dt.strftime("%m/%d %H:%M")
                # Color based on status
                if task.status == "in_progress":
                    time_str = f"{ANSI_RED}{time_str}{ANSI_RESET}"
                elif task.status == "pending":
                    time_str = f"{ANSI_YELLOW}{time_str}{ANSI_RESET}"
                else:  # completed
                    time_str = f"{ANSI_GREEN}{time_str}{ANSI_RESET}"
                parts.append(time_str)
            return " ".join(parts) if parts else ""

        def print_task_details(task, detail_prefix):
            """Print plan and notes for a task."""
            if succinct:
                return

            # Apply strikethrough + dim to completed task details
            is_completed = task.status == "completed"
            def fmt(text):
                if is_completed:
                    return f"{ANSI_DIM}{ANSI_STRIKE}{text}{ANSI_RESET}"
                return text

            def fmt_green(text):
                return f"{ANSI_GREEN}{text}{ANSI_RESET}"

            plan, plan_ca_ref = get_task_plan(task)

            # Show plan_ca_ref or plan
            if plan_ca_ref:
                if verbose:
                    print(f"{detail_prefix}{fmt('📄 ' + plan_ca_ref)}")
                else:
                    print(f"{detail_prefix}{fmt('→ ' + truncate(plan_ca_ref, 60))}")
            elif plan:
                if verbose:
                    for line in plan.split('\n'):
                        print(f"{detail_prefix}{fmt('📋 ' + line)}")
                else:
                    print(f"{detail_prefix}{fmt('→ ' + truncate(plan, 60))}")

            # Show notes
            notes = get_task_notes(task)
            for note in notes:
                if verbose:
                    print(f"{detail_prefix}{fmt('📝 ' + note.description)}")
                    if note.breadcrumb:
                        print(f"{detail_prefix}{fmt('   🔖 ' + note.breadcrumb)}")
                else:
                    print(f"{detail_prefix}{fmt('📝 ' + truncate(note.description, 60))}")

            # In verbose mode, show all updates (not just notes, excluding completion reports)
            if verbose and task.mtmd and task.mtmd.updates:
                lifecycle_updates = [u for u in task.mtmd.updates
                                   if getattr(u, 'type', None) not in ('note', 'completion')]
                for update in lifecycle_updates:
                    desc = update.description or "(lifecycle update)"
                    print(f"{detail_prefix}{fmt('🔄 ' + desc)}")
                    if update.breadcrumb:
                        print(f"{detail_prefix}{fmt('   🔖 ' + update.breadcrumb)}")

            # Always show completion reports (both modes)
            # Last completion report = green, previous = strikethrough
            completion_reports = []
            if task.mtmd:
                # Get completion reports from updates with type='completion'
                for u in (task.mtmd.updates or []):
                    if getattr(u, 'type', None) == 'completion' and u.description:
                        completion_reports.append((u.description, u.breadcrumb))
                # Always check completion_report field (may be the primary source)
                if task.mtmd.completion_report:
                    bc = getattr(task.mtmd, 'completion_breadcrumb', None)
                    completion_reports.append((task.mtmd.completion_report, bc))

            for i, (report, breadcrumb) in enumerate(completion_reports):
                is_last = (i == len(completion_reports) - 1)
                if is_last:
                    # Last completion report: green
                    if verbose:
                        print(f"{detail_prefix}{fmt_green('✅ ' + report)}")
                        if breadcrumb:
                            print(f"{detail_prefix}{fmt_green('   🔖 ' + breadcrumb)}")
                    else:
                        print(f"{detail_prefix}{fmt_green('✅ ' + truncate(report, 60))}")
                else:
                    # Previous completion reports: strikethrough
                    if verbose:
                        print(f"{detail_prefix}{fmt('✅ ' + report)}")
                        if breadcrumb:
                            print(f"{detail_prefix}{fmt('   🔖 ' + breadcrumb)}")
                    else:
                        print(f"{detail_prefix}{fmt('✅ ' + truncate(report, 60))}")

        def print_tree(task, prefix="", is_last=True, depth=0, siblings=None):
            connector = "└── " if is_last else "├── "
            extension = "    " if is_last else "│   "

            # CC-style markers with colors - subject now contains #N prefix
            suffix = format_task_suffix(task)
            if task.status == "completed":
                status_icon = f"{ANSI_GREEN}✔{ANSI_RESET}"
                text = f"{ANSI_DIM}{ANSI_STRIKE}{task.subject}{ANSI_RESET}"
            elif task.status == "in_progress":
                status_icon = f"{ANSI_RED}◼{ANSI_RESET}"
                text = _dim_task_ids(task.subject)
            else:
                status_icon = "◻"
                text = _dim_task_ids(task.subject)

            # Append suffix (repo, version, timestamp) after subject
            if suffix:
                text = f"{text} {suffix}"

            print(f"{prefix}{connector}{status_icon} {text}")

            # Print task details (plan, notes) with proper indentation
            detail_prefix = prefix + extension + "   "  # Extra indent beyond task header
            print_task_details(task, detail_prefix)

            children = get_children(task.id)
            # Filter children in succinct mode
            visible_children = [c for c in children if should_show_task(c, children, depth + 1)]

            for i, child in enumerate(visible_children):
                print_tree(child, prefix + extension, i == len(visible_children) - 1, depth + 1, children)

        # Print header
        total = 1 + count_descendants(root_id)
        print(f"🌳 Task Tree from #{root_id} ({total} tasks)")
        print("=" * 60)

        # Print root specially with CC-style markers - subject now contains #N prefix
        root_suffix = format_task_suffix(root)
        if root.status == "completed":
            status_icon = f"{ANSI_GREEN}✔{ANSI_RESET}"
            root_text = f"{ANSI_DIM}{ANSI_STRIKE}{root.subject}{ANSI_RESET}"
        elif root.status == "in_progress":
            status_icon = f"{ANSI_RED}◼{ANSI_RESET}"
            root_text = _dim_task_ids(root.subject)
        else:
            status_icon = "◻"
            root_text = _dim_task_ids(root.subject)
        if root_suffix:
            root_text = f"{root_text} {root_suffix}"
        print(f"{status_icon} {root_text}")

        # Print root task details (plan, notes) - extra indent beyond header
        print_task_details(root, "      ")

        # Print children
        children = get_children(root_id)
        visible_children = [c for c in children if should_show_task(c, children, 1)]
        for i, child in enumerate(visible_children):
            print_tree(child, "", i == len(visible_children) - 1, depth=1, siblings=children)

        return True

    def get_tasks_mtime(tasks_dir: Path) -> float:
        """Get latest modification time of any file in tasks directory."""
        try:
            if not tasks_dir.exists():
                return 0.0

            # Get mtime of all JSON files in session subdirectories
            mtimes = []
            for session_dir in tasks_dir.iterdir():
                if session_dir.is_dir():
                    for task_file in session_dir.glob("*.json"):
                        mtimes.append(task_file.stat().st_mtime)

            return max(mtimes) if mtimes else 0.0
        except Exception:
            return 0.0

    # Parse task ID (preserve string IDs like "000")
    task_id_str = args.task_id.lstrip('#')
    # Keep as string if it has leading zeros, otherwise normalize
    if task_id_str.startswith('0') and len(task_id_str) > 1:
        root_id = task_id_str  # Preserve leading zeros (e.g., "000")
    else:
        root_id = task_id_str  # Keep as string for consistent comparison

    # Loop mode - monitor for changes
    if args.loop:
        reader = TaskReader()
        tasks_dir = reader.tasks_dir
        last_mtime = 0.0

        try:
            while True:
                current_mtime = get_tasks_mtime(tasks_dir)

                # Display tree if tasks changed or first iteration
                if current_mtime != last_mtime:
                    # Clear screen using ANSI escape code (works on macOS/Linux)
                    print("\033[2J\033[H", end="")

                    if not display_tree(root_id):
                        return 1

                    print()  # Add blank line
                    print(f"{ANSI_DIM}[Monitoring for changes... Press Ctrl+C to exit]{ANSI_RESET}")
                    last_mtime = current_mtime

                time.sleep(1)  # Poll every second

        except KeyboardInterrupt:
            print()  # Clean newline after Ctrl+C
            return 0

    # Normal mode - single display
    return 0 if display_tree(root_id) else 1


def cmd_task_delete(args: argparse.Namespace) -> int:
    """Delete one or more tasks with set-matching grant authorization.

    Requires grant-delete to have been run first with EXACTLY the same task IDs.
    Temporarily unprotects directory for deletion, then re-protects.
    """
    import os
    import stat
    from .task import TaskReader
    from .task.protection import check_grant_in_events, clear_grant
    from .task.create import SENTINEL_TASK_ID

    # Parse all task IDs (handle #N or N format, keep as strings)
    task_ids = []
    for tid_raw in args.task_ids:
        tid_str = str(tid_raw).lstrip('#')
        task_ids.append(tid_str)

    # Block deletion of sentinel task
    filtered_ids = []
    for tid in task_ids:
        if tid == SENTINEL_TASK_ID:
            print(f"⚠️  Skipping sentinel task #{SENTINEL_TASK_ID}")
        else:
            filtered_ids.append(tid)

    if not filtered_ids:
        print("❌ No valid tasks to delete")
        return 1

    # Check for delete grant - sets must match EXACTLY
    has_grant, grant_event = check_grant_in_events("delete", filtered_ids)
    if not has_grant:
        id_list = " ".join(filtered_ids)
        print(f"❌ Delete requires grant authorization")
        print(f"   Run: macf_tools task grant-delete {id_list}")
        print(f"   (Grant must match EXACTLY the tasks to delete)")
        return 1

    # Verify tasks exist
    reader = TaskReader()
    to_delete = []
    for tid in filtered_ids:
        task = reader.read_task(tid)
        if task:
            print(f"🗑️  #{tid}: {task.subject[:60]}")
            to_delete.append(tid)
        else:
            print(f"⚠️  #{tid}: not found, skipping")

    if not to_delete:
        print("❌ No tasks found to delete")
        return 1

    # Confirmation (basic CLI protection)
    if not args.force:
        print()
        confirm = input(f"⚠️  Delete {len(to_delete)} task(s)? [y/N]: ").strip().lower()
        if confirm != 'y':
            print("❌ Cancelled")
            return 1

    # Temporarily unprotect directory for deletion
    session_path = reader.session_path
    if session_path and session_path.exists():
        current_mode = session_path.stat().st_mode
        if not (current_mode & stat.S_IWUSR):
            os.chmod(session_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)  # 755

    try:
        deleted = 0
        for tid in to_delete:
            task_file = reader.session_path / f"{tid}.json"
            if task_file.exists():
                task_file.unlink()
                deleted += 1
                print(f"   ✓ Deleted #{tid}")
    finally:
        # Re-protect directory
        if session_path and session_path.exists():
            os.chmod(session_path, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)  # 555

    # Clear the grant after use
    clear_grant("delete", filtered_ids, reason="consumed by delete")

    print(f"\n✅ Deleted {deleted} task(s)")
    return 0


def cmd_task_edit(args: argparse.Namespace) -> int:
    """Edit a top-level JSON field in a task file."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    field = args.field
    value = args.value

    # Validate field is editable
    # Note: 'subject' is NOT directly editable - it's composed from task_id, parent_id, type, and title
    # To change the title portion, use: macf_tools task metadata set title "New Title"
    if field == "subject":
        print(f"❌ Direct subject editing is not allowed")
        print(f"   Subject is composed from task metadata (id, parent, type, title)")
        print(f"   To change the title: macf_tools task metadata set {task_id_str} title \"New Title\"")
        return 1

    # Block direct status editing - use lifecycle commands instead
    if field == "status":
        print(f"❌ Direct status editing is not allowed")
        print(f"   Use lifecycle commands instead:")
        print(f"   • macf_tools task start {task_id_str}    → in_progress")
        print(f"   • macf_tools task pause {task_id_str}    → pending")
        print(f"   • macf_tools task complete {task_id_str} → completed")
        print(f"   • macf_tools task archive {task_id_str}  → archived")
        return 1

    # Block direct description editing - preserves MTMD metadata
    if field == "description":
        print(f"❌ Direct description editing is not allowed")
        print(f"   Description contains MTMD metadata set during creation.")
        print(f"   Use structured commands instead:")
        print(f"   • macf_tools task note {task_id_str} \"message\"  → append notes")
        print(f"   • macf_tools task edit {task_id_str} plan \"ref\" → update plan reference")
        return 1

    editable_fields = []
    if field not in editable_fields:
        print(f"❌ Field '{field}' is not editable")
        return 1

    # Validate status values
    # Note: "archived" is not a CC-native status but we allow it - CC UI will hide these tasks
    if field == "status" and value not in ["pending", "in_progress", "completed", "archived"]:
        print(f"❌ Invalid status value: {value}")
        print("   Valid values: pending, in_progress, completed, archived")
        return 1

    # Read task to verify it exists and get current state
    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    # For MTMD-aware tasks, add update record when editing description
    if field == "description" and task.mtmd:
        breadcrumb = get_breadcrumb()
        new_mtmd = task.mtmd.with_updated_field("description", "(edited)", breadcrumb, f"Description replaced via CLI")
        # Check if new description already has MTMD (user provided it)
        if "<macf_task_metadata" not in value:
            # Append updated MTMD to NEW description (preserving update history)
            mtmd_block = f'<macf_task_metadata version="{new_mtmd.version}">\n{new_mtmd.to_yaml()}</macf_task_metadata>'
            value = f"{value}\n\n{mtmd_block}"
        # else: user provided MTMD in their description, use as-is

    # Apply update
    if update_task_file(task_id, {field: value}):
        print(f"✅ Updated task #{task_id}")
        print(f"   {field} = {value[:50]}{'...' if len(str(value)) > 50 else ''}")
        return 0
    else:
        print(f"❌ Failed to update task #{task_id}")
        return 1


def cmd_task_metadata_get(args: argparse.Namespace) -> int:
    """Display MTMD for a task (pure metadata output)."""
    from .task import TaskReader

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    # Read task
    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    if not task.mtmd:
        print(f"⚠️  Task #{task_id} has no MTMD")
        return 0

    # Output MTMD
    mtmd = task.mtmd
    print(f"ℹ️ MacfTaskMetaData (v{mtmd.version}) for #{task_id}")
    print("-" * 40)
    if mtmd.creation_breadcrumb:
        print(f"  creation_breadcrumb: {mtmd.creation_breadcrumb}")
    if mtmd.created_cycle:
        print(f"  created_cycle: {mtmd.created_cycle}")
    if mtmd.created_by:
        print(f"  created_by: {mtmd.created_by}")
    if mtmd.parent_id:
        print(f"  parent_id: {mtmd.parent_id}")
    if mtmd.plan_ca_ref:
        print(f"  plan_ca_ref: {mtmd.plan_ca_ref}")
    if mtmd.experiment_ca_ref:
        print(f"  experiment_ca_ref: {mtmd.experiment_ca_ref}")
    if mtmd.repo:
        print(f"  repo: {mtmd.repo}")
    if mtmd.target_version:
        print(f"  target_version: {mtmd.target_version}")
    if mtmd.release_branch:
        print(f"  release_branch: {mtmd.release_branch}")
    if mtmd.completion_breadcrumb:
        print(f"  completion_breadcrumb: {mtmd.completion_breadcrumb}")
    if mtmd.unblock_breadcrumb:
        print(f"  unblock_breadcrumb: {mtmd.unblock_breadcrumb}")
    if mtmd.archived:
        print(f"  archived: {mtmd.archived}")
    if mtmd.archived_at:
        print(f"  archived_at: {mtmd.archived_at}")
    if mtmd.custom:
        print(f"  custom:")
        for k, v in mtmd.custom.items():
            print(f"    {k}: {v}")
    if mtmd.updates:
        print(f"  updates: ({len(mtmd.updates)})")
        for u in mtmd.updates:
            marker = "📝" if getattr(u, 'type', None) == "note" else "•"
            print(f"    {marker} {u.breadcrumb} - {u.description}")

    return 0


def cmd_task_metadata_set(args: argparse.Namespace) -> int:
    """Set an MTMD field within a task's description."""
    from .task import TaskReader, update_task_file, MacfTaskMetaData
    from .utils.breadcrumbs import get_breadcrumb

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    field = args.field
    value = args.value

    # Read task
    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    # Validate MTMD field exists - use dataclass as single source of truth
    import dataclasses
    mtmd_fields = [f.name for f in dataclasses.fields(MacfTaskMetaData) if f.name not in ("version", "updates")]
    if field not in mtmd_fields:
        print(f"❌ Unknown MTMD field: {field}")
        print(f"   Valid fields: {', '.join(sorted(mtmd_fields))}")
        return 1

    # Parse value types for specific fields
    if field == "parent_id" and value != "null":
        try:
            value = int(value)
        except ValueError:
            print(f"❌ parent_id must be an integer")
            return 1
    elif field == "created_cycle" and value != "null":
        try:
            value = int(value)
        except ValueError:
            print(f"❌ created_cycle must be an integer")
            return 1
    elif field == "archived":
        value = value.lower() in ("true", "1", "yes")
    elif value == "null":
        value = None

    # Protection check: modifying existing value requires grant
    if task.mtmd:
        old_val = getattr(task.mtmd, field, None)
        if old_val is not None and old_val != value:
            # Changing existing value - check for grant with field/value specificity
            from .task.protection import check_grant_in_events, clear_grant
            has_grant, _ = check_grant_in_events("update", task_id, field=field, value=value)
            if not has_grant:
                print(f"❌ Modifying MTMD field '{field}' requires grant (current value: {old_val!r})")
                print(f"   To authorize: macf_tools task grant-update {task_id} --field {field} --value \"{value}\"")
                return 1
            # Clear the grant (single-use)
            clear_grant("update", task_id, "consumed_by_metadata_set")

    # Get or create MTMD
    breadcrumb = get_breadcrumb()
    if task.mtmd:
        new_mtmd = task.mtmd.with_updated_field(field, value, breadcrumb, f"Set {field} via CLI")
    else:
        # Create new MTMD with just this field
        new_mtmd = MacfTaskMetaData()
        setattr(new_mtmd, field, value)
        from .task.models import MacfTaskUpdate
        new_mtmd.updates.append(MacfTaskUpdate(
            breadcrumb=breadcrumb,
            description=f"Created MTMD with {field} via CLI",
            agent="PA"
        ))

    # Embed updated MTMD in description
    new_description = task.description_with_updated_mtmd(new_mtmd)

    # Build updates dict
    updates = {"description": new_description}

    # If title or other subject-affecting fields changed, recompose subject
    if field in ("title", "task_type", "parent_id"):
        from .task.create import compose_subject
        # Use new MTMD values for recomposition
        new_subject = compose_subject(
            task_id=str(task_id),
            task_type=new_mtmd.task_type,
            title=new_mtmd.title or value if field == "title" else new_mtmd.title,
            parent_id=new_mtmd.parent_id
        )
        updates["subject"] = new_subject

    # Apply update
    if update_task_file(task_id, updates):
        print(f"✅ Updated MTMD for task #{task_id}")
        print(f"   {field} = {value}")
        if "subject" in updates:
            print(f"   subject recomposed")
        return 0
    else:
        print(f"❌ Failed to update task #{task_id}")
        return 1


def cmd_task_metadata_add(args: argparse.Namespace) -> int:
    """Add a custom field to MTMD's custom section."""
    from .task import TaskReader, update_task_file, MacfTaskMetaData
    from .utils.breadcrumbs import get_breadcrumb

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    key = args.key
    value = args.value

    # Read task
    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    # Get or create MTMD
    breadcrumb = get_breadcrumb()
    if task.mtmd:
        new_mtmd = task.mtmd.with_custom_field(key, value, breadcrumb)
    else:
        # Create new MTMD with custom field
        new_mtmd = MacfTaskMetaData()
        new_mtmd.custom[key] = value
        from .task.models import MacfTaskUpdate
        new_mtmd.updates.append(MacfTaskUpdate(
            breadcrumb=breadcrumb,
            description=f"Created MTMD with custom.{key} via CLI",
            agent="PA"
        ))

    # Embed updated MTMD in description
    new_description = task.description_with_updated_mtmd(new_mtmd)

    # Apply update
    if update_task_file(task_id, {"description": new_description}):
        print(f"✅ Added custom field to task #{task_id}")
        print(f"   custom.{key} = {value}")
        return 0
    else:
        print(f"❌ Failed to update task #{task_id}")
        return 1


def cmd_task_create_mission(args: argparse.Namespace) -> int:
    """Create MISSION task with roadmap folder."""
    from .task.create import create_mission

    # Parse parent ID (normalize)
    parent_id = args.parent.lstrip('#') if args.parent else "000"

    try:
        result = create_mission(
            title=args.title,
            parent_id=parent_id,
            repo=args.repo,
            version=args.version
        )

        if args.json:
            # JSON output for automation
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "folder_path": result.folder_path,
                "ca_path": result.ca_path,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "plan_ca_ref": result.mtmd.plan_ca_ref,
                    "repo": result.mtmd.repo,
                    "target_version": result.mtmd.target_version
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-friendly output
            print(f"✅ Created MISSION task #{result.task_id}")
            print(f"📁 Folder: {result.folder_path}")
            print(f"📄 Roadmap: {result.ca_path}")
            print(f"🏷️  Subject: {result.subject}")
            print()
            print("Next steps:")
            print("1. Edit roadmap.md to fill in phases")
            print(f"2. Run `macf_tools task get #{result.task_id}` to view task details")

        return 0
    except Exception as e:
        print(f"❌ Failed to create MISSION: {e}")
        return 1


def cmd_task_create_experiment(args: argparse.Namespace) -> int:
    """Create EXPERIMENT task with protocol folder."""
    from .task.create import create_experiment

    # Parse parent ID (normalize)
    parent_id = args.parent.lstrip('#') if args.parent else "000"

    try:
        result = create_experiment(title=args.title, parent_id=parent_id)

        if args.json:
            # JSON output for automation
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "folder_path": result.folder_path,
                "ca_path": result.ca_path,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "plan_ca_ref": result.mtmd.plan_ca_ref
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-friendly output
            print(f"✅ Created EXPERIMENT task #{result.task_id}")
            print(f"📁 Folder: {result.folder_path}")
            print(f"📄 Protocol: {result.ca_path}")
            print(f"🏷️  Subject: {result.subject}")
            print()
            print("Next steps:")
            print("1. Edit protocol.md to fill in hypothesis and method")
            print(f"2. Run `macf_tools task get #{result.task_id}` to view task details")

        return 0
    except Exception as e:
        print(f"❌ Failed to create EXPERIMENT: {e}")
        return 1


def cmd_task_create_detour(args: argparse.Namespace) -> int:
    """Create DETOUR task with roadmap folder."""
    from .task.create import create_detour

    # Parse parent ID (normalize)
    parent_id = args.parent.lstrip('#') if args.parent else "000"

    try:
        result = create_detour(
            title=args.title,
            parent_id=parent_id,
            repo=args.repo,
            version=args.version
        )

        if args.json:
            # JSON output for automation
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "folder_path": result.folder_path,
                "ca_path": result.ca_path,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "plan_ca_ref": result.mtmd.plan_ca_ref,
                    "repo": result.mtmd.repo,
                    "target_version": result.mtmd.target_version
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-friendly output
            print(f"✅ Created DETOUR task #{result.task_id}")
            print(f"📁 Folder: {result.folder_path}")
            print(f"📄 Roadmap: {result.ca_path}")
            print(f"🏷️  Subject: {result.subject}")
            print()
            print("Next steps:")
            print("1. Edit roadmap.md to define detour objectives")
            print(f"2. Run `macf_tools task get #{result.task_id}` to view task details")

        return 0
    except Exception as e:
        print(f"❌ Failed to create DETOUR: {e}")
        return 1


def cmd_task_create_phase(args: argparse.Namespace) -> int:
    """Create phase task under parent."""
    from .task.create import create_phase

    # Parse parent ID
    parent_id_str = args.parent.lstrip('#')
    try:
        parent_id = int(parent_id_str)
    except ValueError:
        print(f"❌ Invalid parent ID: {args.parent}")
        return 1

    # Get plan or plan_ca_ref (XOR enforced by argparse)
    plan = getattr(args, 'plan', None)
    plan_ca_ref = getattr(args, 'plan_ca_ref', None)

    # Parse blocked-by IDs (strip # prefix)
    blocked_by = None
    if getattr(args, 'blocked_by', None):
        blocked_by = [bid.lstrip('#') for bid in args.blocked_by]

    try:
        result = create_phase(parent_id=parent_id, title=args.title, plan=plan, plan_ca_ref=plan_ca_ref, blocked_by=blocked_by)

        if args.json:
            # JSON output for automation
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "parent_id": result.mtmd.parent_id
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-friendly output
            print(f"✅ Created phase task #{result.task_id}")
            print(f"🏷️  Subject: {result.subject}")
            print(f"📎 Parent: #{parent_id}")
            if blocked_by:
                print(f"🚧 Blocked by: {', '.join(f'#{b}' for b in blocked_by)}")
            print()
            print("Next steps:")
            print(f"1. Run `macf_tools task tree #{parent_id}` to see hierarchy")

        return 0
    except Exception as e:
        print(f"❌ Failed to create phase: {e}")
        return 1


def cmd_task_create_bug(args: argparse.Namespace) -> int:
    """Create bug task (standalone or under parent)."""
    from .task.create import create_bug

    # Parse optional parent ID
    parent_id = None
    if args.parent:
        parent_id_str = args.parent.lstrip('#')
        # Preserve string IDs (like "000") or convert numeric
        if parent_id_str.lstrip('0') == '' or not parent_id_str.isdigit():
            parent_id = parent_id_str  # Keep as string
        else:
            parent_id = parent_id_str  # Keep as string for consistency

    # Get plan or plan_ca_ref (XOR enforced in create_bug)
    plan = getattr(args, 'plan', None)
    plan_ca_ref = getattr(args, 'plan_ca_ref', None)

    try:
        result = create_bug(
            title=args.title,
            parent_id=parent_id,
            plan=plan,
            plan_ca_ref=plan_ca_ref
        )

        if args.json:
            # JSON output for automation
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "parent_id": result.mtmd.parent_id
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-friendly output
            print(f"✅ Created BUG task #{result.task_id}")
            print(f"🏷️  Subject: {result.subject}")
            if parent_id:
                print(f"📎 Parent: #{parent_id}")
                print()
                print("Next steps:")
                print(f"1. Run `macf_tools task tree #{parent_id}` to see hierarchy")
            else:
                print()
                print("Next steps:")
                print(f"1. Run `macf_tools task get #{result.task_id}` to view details")
                print("2. Mark in_progress when starting work")

        return 0
    except Exception as e:
        print(f"❌ Failed to create bug: {e}")
        return 1


def cmd_task_create_gh_issue(args: argparse.Namespace) -> int:
    """Create GH_ISSUE task by auto-fetching from GitHub."""
    from .task.create import create_gh_issue

    parent_id = None
    if args.parent:
        parent_id = args.parent.lstrip('#')

    try:
        result = create_gh_issue(
            issue_url=args.issue_url,
            parent_id=parent_id,
        )

        if args.json:
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "parent_id": result.mtmd.parent_id,
                    "custom": result.mtmd.custom,
                }
            }
            print(json.dumps(output, indent=2))
        else:
            custom = result.mtmd.custom
            labels = custom.get("gh_labels", [])
            print(f"✅ Created GH_ISSUE task #{result.task_id}")
            print(f"🏷️  Subject: {result.subject}")
            if labels:
                print(f"🏷️  Labels: {', '.join(labels)}")
            print(f"🔗 {custom.get('gh_url', args.issue_url)}")
            if parent_id:
                print(f"📎 Parent: #{parent_id}")

        return 0
    except Exception as e:
        print(f"❌ Failed to create GH_ISSUE: {e}")
        return 1


def cmd_task_create_deleg(args: argparse.Namespace) -> int:
    """Create DELEG_PLAN task for delegation work."""
    from .task.create import create_deleg

    # Parse optional parent ID
    parent_id = None
    if args.parent:
        parent_id_str = args.parent.lstrip('#')
        parent_id = parent_id_str

    # Get plan or plan_ca_ref (XOR enforced in create_deleg)
    plan = getattr(args, 'plan', None)
    plan_ca_ref = getattr(args, 'plan_ca_ref', None)

    try:
        result = create_deleg(
            title=args.title,
            parent_id=parent_id,
            plan=plan,
            plan_ca_ref=plan_ca_ref
        )

        if args.json:
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by,
                    "parent_id": result.mtmd.parent_id
                }
            }
            print(json.dumps(output, indent=2))
        else:
            print(f"✅ Created DELEG task #{result.task_id}")
            print(f"🏷️  Subject: {result.subject}")
            if parent_id:
                print(f"📎 Parent: #{parent_id}")
            print()
            print("Next steps:")
            print(f"1. Run `macf_tools task get #{result.task_id}` to view details")
            print("2. Mark in_progress when starting delegation")

        return 0
    except Exception as e:
        print(f"❌ Failed to create deleg: {e}")
        return 1


def cmd_task_create_task(args: argparse.Namespace) -> int:
    """Create standalone TASK for general work."""
    from .task.create import create_task

    # Parse parent ID (normalize)
    parent_id = args.parent.lstrip('#') if args.parent else "000"

    # Get plan or plan_ca_ref (XOR enforced by argparse)
    plan = getattr(args, 'plan', None)
    plan_ca_ref = getattr(args, 'plan_ca_ref', None)

    try:
        result = create_task(title=args.title, parent_id=parent_id, plan=plan, plan_ca_ref=plan_ca_ref)

        if args.json:
            # JSON output for automation
            output = {
                "task_id": result.task_id,
                "subject": result.subject,
                "mtmd": {
                    "version": result.mtmd.version,
                    "creation_breadcrumb": result.mtmd.creation_breadcrumb,
                    "created_cycle": result.mtmd.created_cycle,
                    "created_by": result.mtmd.created_by
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-friendly output
            print(f"✅ Created task #{result.task_id}")
            print(f"🏷️  Subject: {result.subject}")
            print()
            print("Next steps:")
            print(f"1. Run `macf_tools task get #{result.task_id}` to view details")
            print(f"2. Mark in_progress when starting work")

        return 0
    except Exception as e:
        print(f"❌ Failed to create task: {e}")
        return 1


def cmd_task_archive(args: argparse.Namespace) -> int:
    """Archive a task (and children by default) to disk."""
    from .task.archive import archive_task

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    cascade = not args.no_cascade

    result = archive_task(task_id, cascade=cascade)

    if not result.success:
        print(f"❌ Archive failed: {result.error}")
        return 1

    if args.json_output:
        import json
        output = {
            "success": True,
            "task_id": result.task_id,
            "archive_path": result.archive_path,
            "children_archived": result.children_archived,
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"✅ Archived task #{result.task_id}")
        print(f"   📦 Archive: {result.archive_path}")
        if result.children_archived:
            print(f"   📦 Children archived: {len(result.children_archived)} tasks")
            for child_id in result.children_archived[:5]:
                print(f"      - #{child_id}")
            if len(result.children_archived) > 5:
                print(f"      ... and {len(result.children_archived) - 5} more")

    return 0


def cmd_task_restore(args: argparse.Namespace) -> int:
    """Restore a task from archive."""
    from .task.archive import restore_task

    result = restore_task(args.archive_path_or_id)

    if not result.success:
        if result.task_json:
            # PermissionError fallback: output task JSON for manual TaskCreate
            print(f"⚠️ MACF: {result.error}")
            print(f"   Original ID: #{result.old_id}")
            print(f"\nTask JSON for TaskCreate:")
            print(result.task_json)
            return 2  # Distinct exit code: recoverable via TaskCreate
        print(f"❌ Restore failed: {result.error}")
        return 1

    if args.json_output:
        import json
        output = {
            "success": True,
            "old_id": result.old_id,
            "new_id": result.new_id,
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"✅ Restored task")
        print(f"   📦 Original ID: #{result.old_id}")
        print(f"   🆕 New ID: #{result.new_id}")
        print()
        print(f"View with: macf_tools task get #{result.new_id}")

    return 0


def cmd_task_archived_list(args: argparse.Namespace) -> int:
    """List archived tasks."""
    from .task.archive import list_archived_tasks

    archives = list_archived_tasks()

    if not archives:
        print("No archived tasks found.")
        return 0

    if args.json_output:
        import json
        print(json.dumps(archives, indent=2))
    else:
        print(f"📦 Archived Tasks ({len(archives)} total)")
        print("-" * 60)
        for arch in archives:
            archived_at = arch.get("archived_at", "unknown")
            if archived_at and archived_at != "unknown":
                # Format datetime
                archived_at = archived_at[:19].replace("T", " ")
            print(f"#{arch['id']:>4} | {archived_at} | {arch['subject'][:40]}")
        print()
        print("Restore with: macf_tools task restore <id_or_path>")

    return 0


def cmd_task_grant_update(args: argparse.Namespace) -> int:
    """Grant permission to update a task's description."""
    from .task.protection import create_grant

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    # Validate --value requires --field
    field = getattr(args, 'field', None)
    value = getattr(args, 'value', None)
    if value is not None and field is None:
        print("❌ --value requires --field to be specified")
        return 1

    create_grant("update", task_id, args.reason, field=field, value=value)
    print(f"✅ Grant created for updating task #{task_id}")
    if field:
        print(f"   Field: {field}")
    if value:
        print(f"   Expected value: {value}")
    if args.reason:
        print(f"   Reason: {args.reason}")
    print("   Grant is single-use and will be cleared after consumption.")
    return 0


def cmd_task_grant_delete(args: argparse.Namespace) -> int:
    """Grant permission to delete one or more tasks (single grant for the set)."""
    from .task.protection import create_grant

    # Parse all task IDs into a normalized set
    task_ids = []
    for task_id_raw in args.task_ids:
        task_id_str = str(task_id_raw).lstrip('#')
        # Keep as string (per Cycle 382 string ID refactor)
        task_ids.append(task_id_str)

    # Create ONE grant for the entire set
    create_grant("delete", task_ids, args.reason)

    if len(task_ids) == 1:
        print(f"✅ Grant created for deleting task #{task_ids[0]}")
    else:
        id_list = ", ".join(f"#{tid}" for tid in task_ids)
        print(f"✅ Grant created for deleting {len(task_ids)} tasks: {id_list}")
    if args.reason:
        print(f"   Reason: {args.reason}")
    print("   Grant is single-use and will be cleared after consumption.")

    return 0


def cmd_task_start(args: argparse.Namespace) -> int:
    """Start work on a task - sets status to in_progress with started_breadcrumb."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb
    import json

    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    if task.status == "in_progress":
        print(f"⚠️  Task #{task_id} is already in_progress")
        return 0

    breadcrumb = get_breadcrumb()

    if task.mtmd:
        from .task.models import MacfTaskUpdate
        import copy
        new_mtmd = copy.deepcopy(task.mtmd)
        new_mtmd.started_breadcrumb = breadcrumb
        new_mtmd.updates.append(MacfTaskUpdate(breadcrumb=breadcrumb, description="Task started via CLI", agent="PA"))
        new_description = task.description_with_updated_mtmd(new_mtmd)
        update_task_file(task_id, {"status": "in_progress", "description": new_description})
    else:
        update_task_file(task_id, {"status": "in_progress"})

    # Emit task lifecycle event for downstream hooks and proxy integration
    task_type = getattr(task.mtmd, 'task_type', None) if task.mtmd else None
    plan_ca_ref = getattr(task.mtmd, 'plan_ca_ref', None) if task.mtmd else None
    append_event("task_started", {
        "task_id": str(task_id),
        "task_type": task_type,
        "breadcrumb": breadcrumb,
        "plan_ca_ref": plan_ca_ref,
    })

    # Auto-inject policies mapped to this task type via manifest
    injected_policies = []
    if task_type:
        from .utils.manifest import get_policies_for_task_type
        from .utils import find_policy_file
        policies = get_policies_for_task_type(task_type)
        for policy_name in policies:
            policy_path = find_policy_file(policy_name)
            if policy_path:
                append_event("policy_injection_activated", {
                    "policy_name": policy_name,
                    "policy_path": str(policy_path),
                    "source": "task_type_auto",
                    "task_id": str(task_id),
                })
                injected_policies.append(policy_name)

    print(f"✅ Task #{task_id} started")
    print(f"   Breadcrumb: {breadcrumb}")
    if injected_policies:
        print(f"   Auto-injected policies: {injected_policies}")
    return 0


def cmd_task_pause(args: argparse.Namespace) -> int:
    """Pause work on a task - sets status back to pending."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb
    from .agent_events_log import append_event
    import json

    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    if task.status == "pending":
        print(f"⚠️  Task #{task_id} is already pending")
        return 0

    breadcrumb = get_breadcrumb()

    if task.mtmd:
        from .task.models import MacfTaskUpdate
        import copy
        new_mtmd = copy.deepcopy(task.mtmd)
        new_mtmd.updates.append(MacfTaskUpdate(breadcrumb=breadcrumb, description="Task paused via CLI", agent="PA"))
        new_description = task.description_with_updated_mtmd(new_mtmd)
        update_task_file(task_id, {"status": "pending", "description": new_description})
    else:
        update_task_file(task_id, {"status": "pending"})

    # Emit task lifecycle event for downstream hooks and proxy integration
    task_type = getattr(task.mtmd, 'task_type', None) if task.mtmd else None
    append_event("task_paused", {
        "task_id": str(task_id),
        "task_type": task_type,
        "breadcrumb": breadcrumb,
    })

    # Clear policy injections that were activated for this task type
    cleared_policies = []
    if task_type:
        from .utils.manifest import get_policies_for_task_type
        policies = get_policies_for_task_type(task_type)
        for policy_name in policies:
            append_event("policy_injection_cleared", {
                "policy_name": policy_name,
                "reason": f"task_paused:{task_id}",
            })
            cleared_policies.append(policy_name)

    print(f"✅ Task #{task_id} paused")
    print(f"   Breadcrumb: {breadcrumb}")
    if cleared_policies:
        print(f"   Cleared policies: {cleared_policies}")
    return 0


def cmd_task_note(args: argparse.Namespace) -> int:
    """Add a note to a task's updates list (type='note')."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb
    from .task.models import MacfTaskUpdate
    import copy

    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    if not task.mtmd:
        print(f"⚠️  Task #{task_id} has no MTMD - cannot add note")
        return 1

    breadcrumb = get_breadcrumb()

    new_mtmd = copy.deepcopy(task.mtmd)
    new_mtmd.updates.append(MacfTaskUpdate(
        breadcrumb=breadcrumb,
        description=args.message,
        agent="PA",
        type="note",
    ))
    new_description = task.description_with_updated_mtmd(new_mtmd)
    update_task_file(task_id, {"description": new_description})

    print(f"📝 Note added to task #{task_id}")
    print(f"   {args.message}")
    print(f"   Breadcrumb: {breadcrumb}")
    return 0


def cmd_task_block(args: argparse.Namespace) -> int:
    """Add blocking relationship: task blocks another task."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb

    task_id_str = args.task_id.lstrip('#')
    target_id_str = args.target_id.lstrip('#')
    try:
        task_id = int(task_id_str)
        target_id = int(target_id_str)
    except ValueError:
        print(f"❌ Invalid task ID(s): {args.task_id} or {args.target_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    # Verify target exists
    target = reader.read_task(target_id)
    if not target:
        print(f"❌ Target task #{target_id} not found")
        return 1

    # Get current blocks array
    current_blocks = task.blocks or []
    if str(target_id) in current_blocks or target_id in current_blocks:
        print(f"⚠️  Task #{task_id} already blocks #{target_id}")
        return 0

    # Add to blocks
    new_blocks = current_blocks + [str(target_id)]
    update_task_file(task_id, {"blocks": new_blocks})

    print(f"✅ Task #{task_id} now blocks #{target_id}")
    print(f"   Breadcrumb: {get_breadcrumb()}")
    return 0


def cmd_task_unblock(args: argparse.Namespace) -> int:
    """Remove blocking relationship."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb

    task_id_str = args.task_id.lstrip('#')
    target_id_str = args.target_id.lstrip('#')
    try:
        task_id = int(task_id_str)
        target_id = int(target_id_str)
    except ValueError:
        print(f"❌ Invalid task ID(s): {args.task_id} or {args.target_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    current_blocks = task.blocks or []
    # Check both str and int forms
    if str(target_id) not in current_blocks and target_id not in current_blocks:
        print(f"⚠️  Task #{task_id} does not block #{target_id}")
        return 0

    # Remove from blocks
    new_blocks = [b for b in current_blocks if str(b) != str(target_id)]
    update_task_file(task_id, {"blocks": new_blocks})

    print(f"✅ Task #{task_id} no longer blocks #{target_id}")
    print(f"   Breadcrumb: {get_breadcrumb()}")
    return 0


def cmd_task_blocked_by(args: argparse.Namespace) -> int:
    """Add blocked-by relationship: task is blocked by another task."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb

    task_id_str = args.task_id.lstrip('#')
    blocker_id_str = args.blocker_id.lstrip('#')
    try:
        task_id = int(task_id_str)
        blocker_id = int(blocker_id_str)
    except ValueError:
        print(f"❌ Invalid task ID(s): {args.task_id} or {args.blocker_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    # Verify blocker exists
    blocker = reader.read_task(blocker_id)
    if not blocker:
        print(f"❌ Blocker task #{blocker_id} not found")
        return 1

    # Get current blockedBy array (Python attr is blocked_by, JSON field is blockedBy)
    current_blocked_by = task.blocked_by or []
    if str(blocker_id) in current_blocked_by or blocker_id in current_blocked_by:
        print(f"⚠️  Task #{task_id} is already blocked by #{blocker_id}")
        return 0

    # Add to blockedBy
    new_blocked_by = current_blocked_by + [str(blocker_id)]
    update_task_file(task_id, {"blockedBy": new_blocked_by})

    print(f"✅ Task #{task_id} is now blocked by #{blocker_id}")
    print(f"   Breadcrumb: {get_breadcrumb()}")
    return 0


def cmd_task_unblocked_by(args: argparse.Namespace) -> int:
    """Remove blocked-by relationship."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb

    task_id_str = args.task_id.lstrip('#')
    blocker_id_str = args.blocker_id.lstrip('#')
    try:
        task_id = int(task_id_str)
        blocker_id = int(blocker_id_str)
    except ValueError:
        print(f"❌ Invalid task ID(s): {args.task_id} or {args.blocker_id}")
        return 1

    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    current_blocked_by = task.blocked_by or []
    if str(blocker_id) not in current_blocked_by and blocker_id not in current_blocked_by:
        print(f"⚠️  Task #{task_id} is not blocked by #{blocker_id}")
        return 0

    # Remove from blockedBy (JSON field name)
    new_blocked_by = [b for b in current_blocked_by if str(b) != str(blocker_id)]
    update_task_file(task_id, {"blockedBy": new_blocked_by})

    print(f"✅ Task #{task_id} is no longer blocked by #{blocker_id}")
    print(f"   Breadcrumb: {get_breadcrumb()}")
    return 0


def _gh_issue_closeout(task_id: int, mtmd, args, breadcrumb: str) -> None:
    """Post close-out comment and close GitHub issue.

    The --report is the agent's conscious, professional contribution — passed
    through as the comment body. Automation adds structured metadata (commits,
    verification) and a calling card footer for agent traceability.

    Failures are warnings, not errors — the task is already marked complete.
    """
    import subprocess as _subprocess

    custom = mtmd.custom
    gh_owner = custom.get("gh_owner")
    gh_repo = custom.get("gh_repo")
    gh_issue_number = custom.get("gh_issue_number")

    if not (gh_owner and gh_repo and gh_issue_number):
        print("   ⚠️  Missing GitHub metadata — skipping GitHub closeout")
        return

    # Resolve agent identity for calling card
    try:
        from .utils.identity import get_agent_identity
        agent_name = get_agent_identity()
    except Exception:
        agent_name = "unknown"

    repo_slug = f"{gh_owner}/{gh_repo}"

    # Compose close-out comment:
    # - Report body (agent's conscious contribution)
    # - Structured commits and verification
    # - Calling card footer
    comment_lines = ["## Close-out Report", ""]
    comment_lines.append(args.report)

    if args.commit:
        comment_lines.append("")
        comment_lines.append("**Commits:**")
        for c in args.commit:
            comment_lines.append(f"- [`{c[:8]}`](https://github.com/{repo_slug}/commit/{c})")

    if args.verified:
        comment_lines.append("")
        comment_lines.append(f"**Verification:** {args.verified}")

    comment_lines.append("")
    comment_lines.append("---")
    comment_lines.append(f"*[{agent_name}: task#{task_id} {breadcrumb}]*")

    comment_body = "\n".join(comment_lines)

    # Post comment
    try:
        comment_result = _subprocess.run(
            ["gh", "issue", "comment", str(gh_issue_number),
             "--repo", repo_slug,
             "--body", comment_body],
            capture_output=True, text=True, timeout=15
        )
        if comment_result.returncode == 0:
            print(f"   📝 Close-out comment posted to {repo_slug}#{gh_issue_number}")
        else:
            print(f"   ⚠️  Failed to post comment: {comment_result.stderr.strip()}")
    except FileNotFoundError:
        print("   ⚠️  gh CLI not found — skipping GitHub comment")
    except _subprocess.TimeoutExpired:
        print("   ⚠️  gh CLI timed out — skipping GitHub comment")

    # Close issue
    try:
        close_result = _subprocess.run(
            ["gh", "issue", "close", str(gh_issue_number),
             "--repo", repo_slug,
             "--reason", "completed"],
            capture_output=True, text=True, timeout=15
        )
        if close_result.returncode == 0:
            print(f"   🔒 Issue {repo_slug}#{gh_issue_number} closed")
        else:
            stderr = close_result.stderr.strip()
            if "already closed" in stderr.lower():
                print(f"   ℹ️  Issue {repo_slug}#{gh_issue_number} already closed")
            else:
                print(f"   ⚠️  Failed to close issue: {stderr}")
    except FileNotFoundError:
        print("   ⚠️  gh CLI not found — skipping issue close")
    except _subprocess.TimeoutExpired:
        print("   ⚠️  gh CLI timed out — skipping issue close")


def cmd_task_complete(args: argparse.Namespace) -> int:
    """Mark task complete with mandatory report, breadcrumb, and status change."""
    from .task import TaskReader, update_task_file
    from .utils.breadcrumbs import get_breadcrumb
    import json

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    # Check report is provided
    if not args.report:
        print("❌ Completion report is MANDATORY")
        print()
        print("   The --report flag documents work done, difficulties, future work, and git status.")
        print()
        print("   For format guidance:")
        print("   macf_tools policy navigate task_management")
        print("   (See section on Completion Protocol)")
        print()
        print("   Example:")
        print('   macf_tools task complete #67 --report "Implemented X. No difficulties. Committed: abc1234"')
        return 1

    # Read task
    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    if task.status == "completed":
        print(f"⚠️  Task #{task_id} is already completed")
        return 1

    # Type-specific completion gate: GH_ISSUE
    task_type = getattr(task.mtmd, 'task_type', None) if task.mtmd else None
    if task_type == "GH_ISSUE":
        missing = []
        if not args.commit:
            missing.append("--commit HASH")
        if not args.verified:
            missing.append('--verified "description of verification method"')
        if missing:
            print(f"GH_ISSUE #{task_id} requires structured closeout before completion.")
            print()
            print("To understand requirements, read the \"GH_ISSUE Closeout\" section:")
            print("  macf_tools policy navigate task_management")
            print('  → Look for: "How does GH_ISSUE closeout work?"')
            print()
            print(f"Missing: {', '.join(missing)}")
            return 1

    # Generate breadcrumb
    breadcrumb = get_breadcrumb()

    # Update MTMD with completion_breadcrumb and completion_report
    if task.mtmd:
        from .task.models import MacfTaskUpdate
        import copy
        new_mtmd = copy.deepcopy(task.mtmd)
        new_mtmd.completion_breadcrumb = breadcrumb
        new_mtmd.completion_report = args.report
        new_mtmd.updates.append(MacfTaskUpdate(
            breadcrumb=breadcrumb,
            description="Task completed via CLI",
            agent="PA"
        ))
    else:
        from .task.models import MacfTaskMetaData, MacfTaskUpdate
        new_mtmd = MacfTaskMetaData(
            completion_breadcrumb=breadcrumb,
            completion_report=args.report,
            updates=[MacfTaskUpdate(
                breadcrumb=breadcrumb,
                description="Task completed via CLI",
                agent="PA"
            )]
        )

    # Store GH_ISSUE closeout fields in MTMD custom dict
    if task_type == "GH_ISSUE" and (args.commit or args.verified):
        if args.commit:
            new_mtmd.custom["closeout_commits"] = args.commit
        if args.verified:
            new_mtmd.custom["closeout_verified"] = args.verified

    # Embed updated MTMD in description
    new_description = task.description_with_updated_mtmd(new_mtmd)

    # Update task file with status and description
    success = update_task_file(task_id, {
        "status": "completed",
        "description": new_description
    })

    if success:
        # Emit task lifecycle event for downstream hooks and proxy integration
        plan_ca_ref = getattr(new_mtmd, 'plan_ca_ref', None)
        append_event("task_completed", {
            "task_id": str(task_id),
            "task_type": task_type,
            "breadcrumb": breadcrumb,
            "plan_ca_ref": plan_ca_ref,
            "report": args.report,
        })

        print(f"✅ Task #{task_id} marked complete")
        print(f"   Breadcrumb: {breadcrumb}")
        print(f"   Report: {args.report[:80]}{'...' if len(args.report) > 80 else ''}")
        if task_type == "GH_ISSUE":
            if args.commit:
                print(f"   Commits: {', '.join(args.commit)}")
            if args.verified:
                print(f"   Verified: {args.verified[:80]}{'...' if len(args.verified) > 80 else ''}")

            # GitHub integration: post close-out comment and close issue
            _gh_issue_closeout(task_id, new_mtmd, args, breadcrumb)

        return 0
    else:
        print(f"❌ Failed to update task #{task_id}")
        return 1


def cmd_task_metadata_validate(args: argparse.Namespace) -> int:
    """Validate task MTMD against schema requirements."""
    from .task import TaskReader

    # Parse task ID
    task_id_str = args.task_id.lstrip('#')
    try:
        task_id = int(task_id_str)
    except ValueError:
        print(f"❌ Invalid task ID: {args.task_id}")
        return 1

    # Read task
    reader = TaskReader()
    task = reader.read_task(task_id)
    if not task:
        print(f"❌ Task #{task_id} not found")
        return 1

    print(f"🔍 Validating task #{task_id}: {task.subject[:50]}...")
    print()

    errors = []
    warnings = []

    # Check MTMD presence
    if not task.mtmd:
        errors.append("No MTMD block found in description")
        print("❌ VALIDATION FAILED")
        print()
        for err in errors:
            print(f"   ❌ {err}")
        return 1

    mtmd = task.mtmd

    # Detect task type from subject
    subject = task.subject
    task_type = "regular"
    if "🗺️" in subject or "MISSION" in subject:
        task_type = "MISSION"
    elif "🧪" in subject or "EXPERIMENT" in subject:
        task_type = "EXPERIMENT"
    elif "↩️" in subject or "DETOUR" in subject:
        task_type = "DETOUR"
    elif "📋" in subject:
        task_type = "PHASE"
    elif "🐛" in subject or "BUG" in subject:
        task_type = "BUG"
    elif "🔧" in subject:
        task_type = "TASK"

    print(f"   Type: {task_type}")
    print()

    # Required for ALL tasks
    if not mtmd.creation_breadcrumb:
        errors.append("Missing required field: creation_breadcrumb")
    if not mtmd.created_cycle:
        warnings.append("Missing recommended field: created_cycle")
    if not mtmd.created_by:
        warnings.append("Missing recommended field: created_by")

    # Required for MISSION/EXPERIMENT/DETOUR
    if task_type in ("MISSION", "EXPERIMENT", "DETOUR"):
        if not mtmd.plan_ca_ref:
            errors.append(f"{task_type} requires plan_ca_ref (roadmap/protocol path)")

    # Required for PHASE tasks (children)
    if task_type == "PHASE":
        if not mtmd.parent_id:
            errors.append("PHASE task requires parent_id")

    # Check parent reference in subject matches MTMD
    if "[^#" in subject:
        import re
        match = re.search(r'\[\^#(\d+)\]', subject)
        if match:
            subject_parent = int(match.group(1))
            if mtmd.parent_id and mtmd.parent_id != subject_parent:
                errors.append(f"Subject parent [^#{subject_parent}] doesn't match MTMD parent_id={mtmd.parent_id}")
            elif not mtmd.parent_id:
                warnings.append(f"Subject has parent [^#{subject_parent}] but MTMD missing parent_id")

    # Report results
    if errors:
        print("❌ VALIDATION FAILED")
        print()
        for err in errors:
            print(f"   ❌ {err}")
        for warn in warnings:
            print(f"   ⚠️  {warn}")
        return 1
    elif warnings:
        print("⚠️  VALIDATION PASSED (with warnings)")
        print()
        for warn in warnings:
            print(f"   ⚠️  {warn}")
        return 0
    else:
        print("✅ VALIDATION PASSED")
        return 0


def _check_port_available(port: int, host: str = "127.0.0.1") -> tuple:
    """Check if port is available. Returns (available: bool, owner_pid: int|None)."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return True, None
        except OSError:
            pass
    # Port in use — try to find owner via lsof
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            owner_pid = int(result.stdout.strip().split("\n")[0])
            return False, owner_pid
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError):
        pass
    return False, None


def cmd_proxy_start(args: argparse.Namespace) -> int:
    """Start the API proxy."""
    try:
        from macf.proxy.server import is_proxy_running, run_proxy, start_proxy_daemon
    except ImportError as e:
        print("⚠️ Proxy requires aiohttp:")
        print("   pip install aiohttp")
        print(f"\nImport error: {e}")
        return 1

    if is_proxy_running():
        print("⚠️  Proxy is already running")
        print("   Use 'macf_tools proxy stop' to stop it first")
        return 1

    port = getattr(args, 'port', 8019)
    daemonize = getattr(args, 'daemon', False)

    # Pre-check port availability (catches zombies without PID files)
    available, owner_pid = _check_port_available(port)
    if not available:
        print(f"❌ Port {port} is already in use", file=sys.stderr)
        if owner_pid:
            print(f"   Held by PID {owner_pid}", file=sys.stderr)
            print(f"   Fix: kill {owner_pid} && macf_tools proxy start --daemon", file=sys.stderr)
        else:
            print(f"   Fix: lsof -i :{port}  # find the process", file=sys.stderr)
            print(f"        kill <PID> && macf_tools proxy start --daemon", file=sys.stderr)
        return 1

    try:
        if daemonize:
            pid = start_proxy_daemon(port=port)
            print(f"✅ Proxy started (PID {pid}) on port {port}")
            print(f"   Activate: ANTHROPIC_BASE_URL=http://localhost:{port} claude")
            return 0
        else:
            print(f"[proxy] Starting on port {port}...", file=sys.stderr)
            run_proxy(port=port)
            return 0
    except Exception as e:
        print(f"❌ Error starting proxy: {e}", file=sys.stderr)
        return 1


def cmd_proxy_stop(args: argparse.Namespace) -> int:
    """Stop the running proxy."""
    try:
        from macf.proxy.server import stop_proxy, is_proxy_running
    except ImportError as e:
        print(f"Import error: {e}")
        return 1

    if not is_proxy_running():
        print("Proxy is not running")
        return 0

    try:
        if stop_proxy():
            print("✅ Proxy stopped")
            return 0
        else:
            print("⚠️  Proxy was not running")
            return 0
    except Exception as e:
        print(f"❌ Error stopping proxy: {e}")
        return 1


def cmd_proxy_status(args: argparse.Namespace) -> int:
    """Show proxy status."""
    try:
        from macf.proxy.server import get_proxy_status
    except ImportError as e:
        print(f"Import error: {e}")
        return 1

    status = get_proxy_status()
    json_output = getattr(args, 'json_output', False)

    if json_output:
        print(json.dumps(status, indent=2))
    else:
        running = status.get('running', False)
        if running:
            print(f"✅ Proxy running (PID {status['pid']}, port {status['port']})")
            print(f"   Log: {status['log_path']}")
            print(f"   Activate: ANTHROPIC_BASE_URL=http://localhost:{status['port']} claude")
        else:
            print("⭕ Proxy not running")
            print("   Start: macf_tools proxy start --daemon")
    return 0


def cmd_proxy_stats(args: argparse.Namespace) -> int:
    """Show aggregate token/cost statistics."""
    try:
        from macf.proxy.server import get_proxy_stats
    except ImportError as e:
        print(f"Import error: {e}")
        return 1

    stats = get_proxy_stats()
    if "error" in stats:
        print(f"⚠️  {stats['error']}")
        print(f"   Expected at: {stats.get('log_path', 'unknown')}")
        return 1

    print(f"📊 API Proxy Statistics")
    print(f"   Log: {stats['log_path']}")
    print(f"   Requests: {stats['total_requests']}")
    print(f"   Input tokens:  {stats['total_input_tokens']:,}")
    print(f"   Output tokens: {stats['total_output_tokens']:,}")
    print(f"   Cache read:    {stats['total_cache_read']:,}")
    print(f"   Cache create:  {stats['total_cache_creation']:,}")
    print(f"   Avg latency:   {stats['avg_latency_ms']}ms")
    print(f"   Est. cost:     ${stats['estimated_cost_usd']:.4f}")
    if stats.get('models'):
        print(f"   Models: {stats['models']}")
    return 0


def cmd_proxy_log(args: argparse.Namespace) -> int:
    """Show recent API call events."""
    try:
        from macf.proxy.server import get_recent_log
    except ImportError as e:
        print(f"Import error: {e}")
        return 1

    limit = getattr(args, 'limit', 10)
    events = get_recent_log(limit=limit)

    if not events:
        print("No proxy events logged yet")
        return 0

    for event in events:
        etype = event.get("type", "?")
        ts = event.get("ts", 0)
        ts_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "?"

        if etype == "api_request":
            model = event.get("model", "?")
            msgs = event.get("message_count", 0)
            tools = event.get("tool_count", 0)
            sys_chars = event.get("system_prompt_chars", 0)
            print(f"  [{ts_str}] → REQ  model={model}  msgs={msgs}  tools={tools}  sys={sys_chars:,}ch")
        elif etype == "api_response":
            inp = event.get("input_tokens", 0)
            out = event.get("output_tokens", 0)
            lat = event.get("latency_ms", 0)
            stop = event.get("stop_reason", "?")
            cache = event.get("cache_read_input_tokens", 0)
            print(f"  [{ts_str}] ← RESP in={inp:,}  out={out:,}  cache={cache:,}  {lat}ms  stop={stop}")
        else:
            print(f"  [{ts_str}] {json.dumps(event)}")

    return 0


def cmd_search_service_start(args: argparse.Namespace) -> int:
    """Start the search service daemon."""
    try:
        from macf.search_service import SearchService, is_service_running
        from macf.search_service.retrievers.policy_retriever import PolicyRetriever
    except ImportError as e:
        print("⚠️ Search service requires optional dependencies:")
        print("   pip install sqlite-vec sentence-transformers")
        print(f"\nImport error: {e}")
        return 1

    # Check if already running
    if is_service_running():
        print("⚠️  Search service is already running")
        print("   Use 'macf_tools search-service stop' to stop it first")
        return 1

    # Get configuration
    port = getattr(args, 'port', 9001)
    daemonize = getattr(args, 'daemon', False)

    try:
        # Create service and register policy retriever
        service = SearchService(port=port)
        service.register(PolicyRetriever())

        # Start service (blocking unless daemonized)
        print(f"Starting search service on port {port}...", file=sys.stderr)
        if daemonize:
            print("Running in background (daemon mode)", file=sys.stderr)

        service.start(daemonize=daemonize)
        return 0

    except Exception as e:
        print(f"❌ Error starting search service: {e}", file=sys.stderr)
        return 1


def cmd_search_service_stop(args: argparse.Namespace) -> int:
    """Stop the running search service."""
    try:
        from macf.search_service import stop_service, is_service_running
    except ImportError as e:
        print(f"Import error: {e}")
        return 1

    if not is_service_running():
        print("Search service is not running")
        return 0

    try:
        if stop_service():
            print("✅ Search service stopped")
            return 0
        else:
            print("⚠️  Service was not running")
            return 0
    except Exception as e:
        print(f"❌ Error stopping service: {e}")
        return 1


def cmd_search_service_status(args: argparse.Namespace) -> int:
    """Show search service status."""
    try:
        from macf.search_service import get_service_status
    except ImportError as e:
        print(f"Import error: {e}")
        return 1

    try:
        status = get_service_status()
        json_output = getattr(args, 'json_output', False)

        if json_output:
            print(json.dumps(status, indent=2))
        else:
            running = status.get('running', False)
            pid = status.get('pid')
            port = status.get('port', 9001)

            if running:
                print(f"✅ Search service is running")
                print(f"   PID: {pid}")
                print(f"   Port: {port}")
            else:
                print("⚠️  Search service is not running")
                print(f"   Start with: macf_tools search-service start")

        return 0

    except Exception as e:
        print(f"❌ Error getting status: {e}")
        return 1


def cmd_transcripts_search(args: argparse.Namespace) -> int:
    """Search transcripts by breadcrumb with context window."""
    from .forensics.transcript_search import search_by_breadcrumb, search_all_transcripts
    import json as json_lib

    breadcrumb = args.breadcrumb
    before = args.before
    after = args.after
    output_format = args.format

    if args.search_all:
        window = search_all_transcripts(breadcrumb, before, after)
    else:
        window = search_by_breadcrumb(breadcrumb, before, after)

    if not window:
        print(f"❌ Breadcrumb not found: {breadcrumb}")
        return 1

    if output_format == "json":
        result = {
            "breadcrumb": window.breadcrumb,
            "target_index": window.target_index,
            "total_messages": window.total_messages,
            "transcript_path": window.transcript_path,
            "before": [{"index": m.index, "role": m.role, "content": m.content} for m in window.before],
            "target": {"index": window.target_message.index, "role": window.target_message.role, "content": window.target_message.content},
            "after": [{"index": m.index, "role": m.role, "content": m.content} for m in window.after],
        }
        print(json_lib.dumps(result, indent=2))
    elif output_format == "compact":
        print(f"📍 Found at index {window.target_index}/{window.total_messages} in {window.transcript_path}")
        print(f"   Breadcrumb: {window.breadcrumb}")
        for msg in window.all_messages():
            marker = "→" if msg.index == window.target_index else " "
            role_emoji = "👤" if msg.role == "user" else "🤖"
            preview = msg.content[:80].replace("\n", " ")
            print(f"{marker} [{msg.index}] {role_emoji} {preview}...")
    else:  # full
        print(f"{'='*60}")
        print(f"📍 Breadcrumb Search Results")
        print(f"{'='*60}")
        print(f"Breadcrumb: {window.breadcrumb}")
        print(f"Transcript: {window.transcript_path}")
        print(f"Target Index: {window.target_index} / {window.total_messages}")
        print(f"Window: {before} before, {after} after")
        print(f"{'='*60}")
        print()
        for msg in window.all_messages():
            is_target = msg.index == window.target_index
            marker = "🎯 TARGET" if is_target else ""
            role_emoji = "👤 USER" if msg.role == "user" else "🤖 ASSISTANT"
            border = "=" if is_target else "-"
            print(f"{border*40} [{msg.index}] {role_emoji} {marker}")
            print(msg.content)
            print()

    return 0


def cmd_transcripts_list(args: argparse.Namespace) -> int:
    """List all transcript files."""
    from .forensics.transcript_search import list_all_transcripts
    import json as json_lib

    transcripts = list_all_transcripts()

    if args.json_output:
        print(json_lib.dumps(transcripts, indent=2))
    else:
        print(f"📂 Found {len(transcripts)} transcripts:")
        for path in transcripts:
            print(f"   {path}")

    return 0


# -------- auto-restart handlers --------
def _cmd_ar_launch(args):
    from .supervisor import launch
    cmd = [a for a in args.cmd if a != "--"]
    if not cmd:
        print("Usage: macf_tools auto-restart launch -- <command> [args...]")
        return 1
    return launch(cmd, name=args.name, restart_delay=args.delay,
                  new_window=getattr(args, 'new_window', False),
                  terminal=getattr(args, 'terminal', 'auto'))

def _cmd_ar_list(args=None):
    from .supervisor import list_processes
    show_all = getattr(args, 'show_all', False) if args else False
    list_processes(show_all=show_all)
    return 0

def _cmd_ar_restart(args):
    from .supervisor import restart
    restart(args.pid)
    return 0

def _cmd_ar_disable(args):
    from .supervisor import disable
    disable(args.pid)
    return 0

def _cmd_ar_status(args):
    from .supervisor import status
    status(args.pid)
    return 0

def _cmd_ar_kill(args):
    from .supervisor import kill_process
    kill_process(args.pid)
    return 0


# -------- parser --------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="macf_tools", description="macf demo CLI (no external deps)"
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {_ver}")
    sub = p.add_subparsers(dest="cmd")  # keep non-required for compatibility

    # cmd-tree: introspect parser structure (like unix tree command)
    # Note: parser 'p' captured via closure, passed to cmd_tree at runtime
    sub.add_parser("cmd-tree", help="print command tree structure").set_defaults(
        func=lambda args: cmd_tree(args, root_parser=p)
    )

    env_parser = sub.add_parser("env", help="print environment summary")
    env_parser.add_argument("--json", action="store_true", help="output as JSON")
    env_parser.set_defaults(func=cmd_env)
    sub.add_parser("time", help="print current local time with CCP gap").set_defaults(func=cmd_time)
    sub.add_parser("budget", help="print budget thresholds (JSON)").set_defaults(func=cmd_budget)

    # New consciousness commands
    list_parser = sub.add_parser("list", help="list consciousness artifacts")
    list_sub = list_parser.add_subparsers(dest="list_cmd")
    ccps_parser = list_sub.add_parser("ccps", help="list consciousness checkpoints")
    ccps_parser.add_argument("--recent", type=int, help="limit to N most recent CCPs")
    ccps_parser.set_defaults(func=cmd_list_ccps)

    session_parser = sub.add_parser("session", help="session management")
    session_sub = session_parser.add_subparsers(dest="session_cmd")
    session_sub.add_parser("info", help="show session information").set_defaults(func=cmd_session_info)

    # Hook commands
    hook_parser = sub.add_parser("hooks", help="hook management")
    hook_sub = hook_parser.add_subparsers(dest="hook_cmd")

    install_parser = hook_sub.add_parser("install", help="install compaction detection hook")
    install_parser.add_argument("--local", dest="local_install", action="store_true",
                               help="install to local project (default)")
    install_parser.add_argument("--global", dest="global_install", action="store_true",
                               help="install to global ~/.claude directory")
    install_parser.set_defaults(func=cmd_hook_install)

    hook_sub.add_parser("test", help="test compaction detection on current session").set_defaults(func=cmd_hook_test)

    logs_parser = hook_sub.add_parser("logs", help="display hook event logs")
    logs_parser.add_argument("--session", help="specific session ID (default: current)")
    logs_parser.set_defaults(func=cmd_hook_logs)

    hook_sub.add_parser("status", help="display current hook states").set_defaults(func=cmd_hook_status)

    # Framework commands (unified installation of hooks, commands, skills)
    framework_parser = sub.add_parser("framework", help="framework artifact management")
    framework_sub = framework_parser.add_subparsers(dest="framework_cmd")

    fw_install = framework_sub.add_parser("install", help="install framework artifacts (hooks, commands, skills)")
    fw_install.add_argument("--hooks-only", dest="hooks_only", action="store_true",
                           help="install only hooks (backward compatibility)")
    fw_install.add_argument("--skip-hooks", dest="skip_hooks", action="store_true",
                           help="skip hook installation (commands and skills only)")
    fw_install.set_defaults(func=cmd_framework_install)

    # Config commands
    config_parser = sub.add_parser("config", help="agent configuration management")
    config_sub = config_parser.add_subparsers(dest="config_cmd")

    init_parser = config_sub.add_parser("init", help="initialize agent configuration")
    init_parser.add_argument("--force", action="store_true", help="overwrite existing config")
    init_parser.set_defaults(func=cmd_config_init)

    config_sub.add_parser("show", help="show current configuration").set_defaults(func=cmd_config_show)

    # Claude Code configuration commands
    claude_config_parser = sub.add_parser("claude-config", help="Claude Code settings management")
    claude_config_sub = claude_config_parser.add_subparsers(dest="claude_config_cmd")

    claude_config_sub.add_parser("init", help="set recommended defaults (verbose=true, autoCompact=false)").set_defaults(func=cmd_claude_config_init)
    claude_config_sub.add_parser("show", help="show current .claude.json configuration").set_defaults(func=cmd_claude_config_show)

    # Agent commands
    agent_parser = sub.add_parser("agent", help="agent initialization and management")
    agent_sub = agent_parser.add_subparsers(dest="agent_cmd")

    agent_init_parser = agent_sub.add_parser("init", help="initialize agent with PA preamble")
    agent_init_parser.add_argument("-y", "--yes", action="store_true", help="skip confirmation prompt")
    agent_init_parser.set_defaults(func=cmd_agent_init)

    # Agent backup subcommands
    backup_parser = agent_sub.add_parser("backup", help="consciousness backup operations")
    backup_sub = backup_parser.add_subparsers(dest="backup_cmd")

    backup_create = backup_sub.add_parser("create", help="create consciousness backup archive")
    backup_create.add_argument("--output", "-o", type=Path, help="output directory (default: CWD)")
    backup_create.add_argument("--no-transcripts", action="store_true", help="exclude transcripts")
    backup_create.add_argument("--quick", action="store_true", help="only recent transcripts (7 days)")
    backup_create.set_defaults(func=cmd_backup_create)

    backup_list = backup_sub.add_parser("list", help="list backup archives")
    backup_list.add_argument("--dir", type=Path, help="directory to scan (default: CWD)")
    backup_list.add_argument("--json", dest="json_output", action="store_true", help="output as JSON")
    backup_list.set_defaults(func=cmd_backup_list)

    backup_info = backup_sub.add_parser("info", help="show backup archive info")
    backup_info.add_argument("archive", type=Path, help="path to archive")
    backup_info.add_argument("--json", dest="json_output", action="store_true", help="output as JSON")
    backup_info.set_defaults(func=cmd_backup_info)

    # Agent restore subcommands
    restore_parser = agent_sub.add_parser("restore", help="consciousness restore operations")
    restore_sub = restore_parser.add_subparsers(dest="restore_cmd")

    restore_verify = restore_sub.add_parser("verify", help="verify archive integrity")
    restore_verify.add_argument("archive", type=Path, help="path to archive")
    restore_verify.add_argument("-v", "--verbose", action="store_true", help="show missing/corrupted file details")
    restore_verify.set_defaults(func=cmd_restore_verify)

    restore_install = restore_sub.add_parser("install", help="install backup to target")
    restore_install.add_argument("archive", type=Path, help="path to archive")
    restore_install.add_argument("--target", type=Path, help="target directory (default: CWD)")
    restore_install.add_argument("--transplant", action="store_true", help="rewrite paths for new system")
    restore_install.add_argument("--maceff-root", type=Path, help="MacEff location (default: sibling of target)")
    restore_install.add_argument("--force", action="store_true", help="overwrite existing consciousness (creates checkpoint)")
    restore_install.add_argument("--dry-run", action="store_true", help="show what would be done")
    restore_install.set_defaults(func=cmd_restore_install)

    # Context command
    context_parser = sub.add_parser("context", help="show token usage and CL (Context Left) level")
    context_parser.add_argument("--json", dest="json_output", action="store_true",
                               help="output as JSON")
    context_parser.add_argument("--session", help="specific session ID (default: current)")
    context_parser.set_defaults(func=cmd_context)

    # Statusline command with subcommands
    statusline_parser = sub.add_parser("statusline", help="statusline operations for Claude Code")
    statusline_sub = statusline_parser.add_subparsers(dest="statusline_cmd")

    # statusline (default - generate output)
    statusline_generate = statusline_sub.add_parser("generate", help="generate formatted statusline output")
    statusline_generate.set_defaults(func=cmd_statusline)

    # statusline install
    statusline_install = statusline_sub.add_parser("install", help="install statusline script and configure Claude Code")
    statusline_install.set_defaults(func=cmd_statusline_install)

    # Default to generate if no subcommand
    statusline_parser.set_defaults(func=cmd_statusline)

    # Breadcrumb command
    breadcrumb_parser = sub.add_parser("breadcrumb", help="generate fresh breadcrumb for TODO completion")
    breadcrumb_parser.add_argument("--json", dest="json_output", action="store_true",
                                  help="output as JSON with components")
    breadcrumb_parser.set_defaults(func=cmd_breadcrumb)

    # DEV_DRV forensic command
    dev_drv_parser = sub.add_parser("dev_drv", help="extract and display DEV_DRV from JSONL")
    dev_drv_parser.add_argument("--breadcrumb", required=True,
                               help="breadcrumb string like s_abc12345/c_42/g_abc1234/p_def5678/t_1234567890")
    dev_drv_parser.add_argument("--raw", action="store_true",
                               help="output raw JSONL (default: markdown summary)")
    dev_drv_parser.add_argument("--md", action="store_true",
                               help="output markdown summary (default)")
    dev_drv_parser.add_argument("--output", help="output file path (default: stdout)")
    dev_drv_parser.set_defaults(func=cmd_dev_drv)

    # Policy commands
    policy_parser = sub.add_parser("policy", help="policy manifest management")
    policy_sub = policy_parser.add_subparsers(dest="policy_cmd")

    # policy manifest
    manifest_parser = policy_sub.add_parser("manifest", help="display merged and filtered policy manifest")
    manifest_parser.add_argument("--format", choices=["json", "summary"], default="summary",
                                help="output format (default: summary)")
    manifest_parser.set_defaults(func=cmd_policy_manifest)

    # policy search
    search_parser = policy_sub.add_parser("search", help="search for keyword in policy manifest")
    search_parser.add_argument("keyword", help="keyword to search for")
    search_parser.set_defaults(func=cmd_policy_search)

    # policy navigate
    navigate_parser = policy_sub.add_parser("navigate", help="show CEP navigation guide (up to boundary)")
    navigate_parser.add_argument("policy_name", help="policy name (e.g., todo_hygiene, development/todo_hygiene)")
    navigate_parser.set_defaults(func=cmd_policy_navigate)

    # policy read
    read_parser = policy_sub.add_parser("read", help="read policy with line numbers and caching")
    read_parser.add_argument("policy_name", help="policy name (e.g., todo_hygiene, development/todo_hygiene)")
    read_parser.add_argument("--lines", help="line range START:END (e.g., 50:100)")
    read_parser.add_argument("--section", help="section number to read (e.g., 5, 5.1) - includes subsections")
    read_parser.add_argument("--force", action="store_true", help="bypass cache for full read")
    read_parser.add_argument("--from-nav-boundary", action="store_true", help="start after CEP_NAV_BOUNDARY (use after navigate)")
    read_parser.set_defaults(func=cmd_policy_read)

    # policy list
    list_parser = policy_sub.add_parser("list", help="list policy files from framework")
    list_parser.add_argument("--tier", choices=["CORE", "optional"], help="filter by tier")
    list_parser.add_argument("--category", help="filter by category (development, consciousness, meta)")
    list_parser.set_defaults(func=cmd_policy_list)

    # policy ca-types
    ca_types_parser = policy_sub.add_parser("ca-types", help="show CA types with emojis")
    ca_types_parser.set_defaults(func=cmd_policy_ca_types)

    # policy recommend
    recommend_parser = policy_sub.add_parser("recommend", help="hybrid search policy recommendations")
    recommend_parser.add_argument("query", help="natural language query (minimum 10 chars)")
    recommend_parser.add_argument("--json", dest="json_output", action="store_true",
                                  help="output as JSON for automation")
    recommend_parser.add_argument("--explain", action="store_true",
                                  help="show detailed retriever breakdown")
    recommend_parser.add_argument("--limit", type=int, default=5,
                                  help="max results to show (default: 5)")
    recommend_parser.set_defaults(func=cmd_policy_recommend)

    # policy build_index
    build_index_parser = policy_sub.add_parser("build_index", help="build hybrid FTS5 + semantic index")
    build_index_parser.add_argument("--policies-dir", help="path to policies directory")
    build_index_parser.add_argument("--db-path", help="output database path")
    build_index_parser.add_argument("--skip-embeddings", action="store_true",
                                    help="skip embedding generation (FTS5 only)")
    build_index_parser.add_argument("--json", dest="json_output", action="store_true",
                                    help="output stats as JSON")
    build_index_parser.set_defaults(func=cmd_policy_build_index)

    # policy inject
    inject_parser = policy_sub.add_parser("inject", help="activate policy injection into PreToolUse hooks")
    inject_parser.add_argument("policy_name", help="policy name to inject (e.g., task_management)")
    inject_parser.set_defaults(func=cmd_policy_inject)

    # policy clear-injection
    clear_inj_parser = policy_sub.add_parser("clear-injection", help="clear a specific policy injection")
    clear_inj_parser.add_argument("policy_name", help="policy name to clear")
    clear_inj_parser.set_defaults(func=cmd_policy_clear_injection)

    # policy clear-injections
    clear_all_parser = policy_sub.add_parser("clear-injections", help="clear all policy injections")
    clear_all_parser.set_defaults(func=cmd_policy_clear_injections)

    # policy injections
    injections_parser = policy_sub.add_parser("injections", help="list active policy injections")
    injections_parser.set_defaults(func=cmd_policy_injections)

    # Mode commands
    mode_parser = sub.add_parser("mode", help="operating mode management (MANUAL_MODE/AUTO_MODE)")
    mode_sub = mode_parser.add_subparsers(dest="mode_cmd")

    mode_get = mode_sub.add_parser("get", help="get current operating mode")
    mode_get.add_argument("--json", dest="json_output", action="store_true",
                         help="output as JSON")
    mode_get.set_defaults(func=cmd_mode_get)

    mode_set = mode_sub.add_parser("set", help="set operating mode")
    mode_set.add_argument("mode", help="mode to set (AUTO_MODE or MANUAL_MODE)")
    mode_set.add_argument("--auth-token", dest="auth_token",
                         help="auth token for AUTO_MODE activation")
    mode_set.set_defaults(func=cmd_mode_set)

    # Events commands
    events_parser = sub.add_parser("events", help="agent events log management")
    events_sub = events_parser.add_subparsers(dest="events_cmd")

    # events show
    show_parser = events_sub.add_parser("show", help="display current agent state")
    show_parser.add_argument("--json", dest="json_output", action="store_true",
                            help="output as JSON")
    show_parser.set_defaults(func=cmd_events_show)

    # events history
    history_parser = events_sub.add_parser("history", help="show recent events")
    history_parser.add_argument("--limit", type=int, default=10,
                               help="number of events to show (default: 10)")
    history_parser.set_defaults(func=cmd_events_history)

    # events query
    query_parser = events_sub.add_parser("query", help="query events with filters")
    query_parser.add_argument("--event", help="filter by event type")
    query_parser.add_argument("--cycle", help="filter by cycle number")
    query_parser.add_argument("--git-hash", help="filter by git hash")
    query_parser.add_argument("--session", help="filter by session ID")
    query_parser.add_argument("--prompt", help="filter by prompt UUID")
    query_parser.add_argument("--after", help="events after timestamp")
    query_parser.add_argument("--before", help="events before timestamp")
    query_parser.add_argument("--command", help="filter cli_command_invoked by command (e.g., 'policy read')")
    query_parser.add_argument("--verbose", "-v", action="store_true", help="show full event data")
    query_parser.set_defaults(func=cmd_events_query)

    # events query-set
    query_set_parser = events_sub.add_parser("query-set", help="perform set operations on queries")
    query_set_parser.add_argument("--query", help="base query (format: key=value)")
    query_set_parser.add_argument("--subtract", help="subtract query (format: key=value)")
    query_set_parser.set_defaults(func=cmd_events_query_set)

    # events sessions
    sessions_parser = events_sub.add_parser("sessions", help="session analysis")
    sessions_sub = sessions_parser.add_subparsers(dest="sessions_cmd")
    sessions_sub.add_parser("list", help="list all sessions").set_defaults(func=cmd_events_sessions_list)

    # events stats
    events_sub.add_parser("stats", help="display event statistics").set_defaults(func=cmd_events_stats)

    # events gaps
    gaps_parser = events_sub.add_parser("gaps", help="detect time gaps (crashes)")
    gaps_parser.add_argument("--threshold", type=float, default=3600,
                            help="gap threshold in seconds (default: 3600)")
    gaps_parser.set_defaults(func=cmd_events_gaps)

    # Task commands (MACF Task CLI)
    task_parser = sub.add_parser("task", help="task management with MTMD support")
    task_sub = task_parser.add_subparsers(dest="task_cmd")

    # task list
    task_list_parser = task_sub.add_parser("list", help="list tasks with hierarchy")
    task_list_parser.add_argument("--json", dest="json_output", action="store_true",
                                  help="output as JSON")
    task_list_parser.add_argument("--type", dest="type_filter",
                                  choices=["MISSION", "EXPERIMENT", "DETOUR", "PHASE"],
                                  help="filter by task type")
    task_list_parser.add_argument("--status", dest="status_filter",
                                  choices=["pending", "in_progress", "completed"],
                                  help="filter by status")
    task_list_parser.add_argument("--parent", dest="parent_filter", type=int,
                                  help="filter by parent task ID")
    task_list_parser.add_argument("--all", dest="show_all", action="store_true",
                                  help="show all tasks including archived")
    task_list_parser.add_argument("--archived", dest="show_archived_only", action="store_true",
                                  help="show only archived tasks")
    task_list_parser.set_defaults(func=cmd_task_list)

    # task get
    task_get_parser = task_sub.add_parser("get", help="get task details")
    task_get_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_get_parser.add_argument("--json", dest="json_output", action="store_true",
                                 help="output as JSON")
    task_get_parser.set_defaults(func=cmd_task_get)

    # task tree
    task_tree_parser = task_sub.add_parser("tree", help="show task hierarchy tree")
    task_tree_parser.add_argument("task_id", nargs="?", default="000",
                                  help="root task ID (default: 000 sentinel)")
    task_tree_parser.add_argument("--loop", action="store_true",
                                  help="monitor tasks directory and auto-refresh on changes")
    task_tree_parser.add_argument("--succinct", "-s", action="store_true",
                                  help="hide notes/plans, show only active/pending tasks")
    task_tree_parser.add_argument("--verbose", "-v", action="store_true",
                                  help="show full plans, breadcrumbs, and all updates")
    tree_archive_group = task_tree_parser.add_mutually_exclusive_group()
    tree_archive_group.add_argument("--archived", action="store_true",
                                    help="show ONLY archived tasks")
    tree_archive_group.add_argument("--all", action="store_true", dest="show_all",
                                    help="show all tasks including archived (default hides archived)")
    task_tree_parser.set_defaults(func=cmd_task_tree)

    # task delete
    task_delete_parser = task_sub.add_parser("delete", help="delete task(s) (HIGH protection)")
    task_delete_parser.add_argument("task_ids", nargs='+', help="task ID(s) (e.g., #67 or 67, accepts multiple)")
    task_delete_parser.add_argument("--force", "-f", action="store_true",
                                    help="skip confirmation prompt")
    task_delete_parser.set_defaults(func=cmd_task_delete)

    # task edit
    task_edit_parser = task_sub.add_parser("edit", help="edit task JSON field")
    task_edit_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_edit_parser.add_argument("field", help="field to edit (subject, status, description)")
    task_edit_parser.add_argument("value", help="new value for the field")
    task_edit_parser.set_defaults(func=cmd_task_edit)

    # task metadata subcommand
    task_metadata_parser = task_sub.add_parser("metadata", help="MTMD metadata operations")
    task_metadata_sub = task_metadata_parser.add_subparsers(dest="metadata_cmd")

    # task metadata get
    task_metadata_get_parser = task_metadata_sub.add_parser("get", help="display MTMD for a task")
    task_metadata_get_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_metadata_get_parser.set_defaults(func=cmd_task_metadata_get)

    # task metadata set
    task_metadata_set_parser = task_metadata_sub.add_parser("set", help="set MTMD field")
    task_metadata_set_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_metadata_set_parser.add_argument("field", help="MTMD field to set")
    task_metadata_set_parser.add_argument("value", help="new value for the field")
    task_metadata_set_parser.set_defaults(func=cmd_task_metadata_set)

    # task metadata add
    task_metadata_add_parser = task_metadata_sub.add_parser("add", help="add custom MTMD field")
    task_metadata_add_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_metadata_add_parser.add_argument("key", help="custom field key")
    task_metadata_add_parser.add_argument("value", help="custom field value")
    task_metadata_add_parser.set_defaults(func=cmd_task_metadata_add)

    # task metadata validate
    task_metadata_validate_parser = task_metadata_sub.add_parser("validate", help="validate MTMD against schema")
    task_metadata_validate_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_metadata_validate_parser.set_defaults(func=cmd_task_metadata_validate)

    # task create subcommand
    task_create_parser = task_sub.add_parser("create", help="create new tasks with MTMD")
    task_create_sub = task_create_parser.add_subparsers(dest="create_cmd")

    # task create mission
    task_create_mission_parser = task_create_sub.add_parser("mission", help="create MISSION task with roadmap")
    task_create_mission_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_mission_parser.add_argument("title", help="mission title")
    task_create_mission_parser.add_argument("--repo", help="repository name (e.g., MacEff)")
    task_create_mission_parser.add_argument("--version", help="target version (e.g., 0.4.0)")
    task_create_mission_parser.add_argument("--json", dest="json", action="store_true",
                                            help="output as JSON")
    task_create_mission_parser.set_defaults(func=cmd_task_create_mission)

    # task create experiment
    task_create_experiment_parser = task_create_sub.add_parser("experiment", help="create EXPERIMENT task with protocol")
    task_create_experiment_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_experiment_parser.add_argument("title", help="experiment title")
    task_create_experiment_parser.add_argument("--json", dest="json", action="store_true",
                                               help="output as JSON")
    task_create_experiment_parser.set_defaults(func=cmd_task_create_experiment)

    # task create detour
    task_create_detour_parser = task_create_sub.add_parser("detour", help="create DETOUR task with roadmap")
    task_create_detour_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_detour_parser.add_argument("title", help="detour title")
    task_create_detour_parser.add_argument("--repo", help="repository name (e.g., MacEff)")
    task_create_detour_parser.add_argument("--version", help="target version (e.g., 0.4.0)")
    task_create_detour_parser.add_argument("--json", dest="json", action="store_true",
                                           help="output as JSON")
    task_create_detour_parser.set_defaults(func=cmd_task_create_detour)

    # task create phase
    task_create_phase_parser = task_create_sub.add_parser("phase", help="create phase task under parent")
    task_create_phase_parser.add_argument("--parent", required=True, help="parent task ID (e.g., #67 or 67)")
    task_create_phase_parser.add_argument("title", help="phase title")
    # XOR: exactly one of plan or plan_ca_ref required (uniform requirement)
    phase_plan_group = task_create_phase_parser.add_mutually_exclusive_group(required=True)
    phase_plan_group.add_argument("--plan", dest="plan", help="inline plan description")
    phase_plan_group.add_argument("--plan-ca-ref", dest="plan_ca_ref", help="path to plan CA")
    task_create_phase_parser.add_argument("--blocked-by", dest="blocked_by", nargs="+",
                                          help="task IDs that block this phase (e.g., #50 51)")
    task_create_phase_parser.add_argument("--json", dest="json", action="store_true",
                                          help="output as JSON")
    task_create_phase_parser.set_defaults(func=cmd_task_create_phase)

    # task create bug
    task_create_bug_parser = task_create_sub.add_parser("bug", help="create bug task (standalone or under parent)")
    task_create_bug_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_bug_parser.add_argument("title", help="bug title")
    # XOR: exactly one of fix_plan or plan_ca_ref required
    bug_plan_group = task_create_bug_parser.add_mutually_exclusive_group(required=True)
    bug_plan_group.add_argument("--plan", dest="plan", help="inline plan description (simple bugs)")
    bug_plan_group.add_argument("--plan-ca-ref", dest="plan_ca_ref", help="path to BUG_FIX roadmap CA (complex bugs)")
    task_create_bug_parser.add_argument("--json", dest="json", action="store_true",
                                        help="output as JSON")
    task_create_bug_parser.set_defaults(func=cmd_task_create_bug)

    # task create gh_issue
    task_create_gh_issue_parser = task_create_sub.add_parser("gh_issue", help="create task from GitHub issue (auto-fetches metadata)")
    task_create_gh_issue_parser.add_argument("issue_url", help="GitHub issue URL (https://github.com/owner/repo/issues/N)")
    task_create_gh_issue_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_gh_issue_parser.add_argument("--json", dest="json", action="store_true",
                                             help="output as JSON")
    task_create_gh_issue_parser.set_defaults(func=cmd_task_create_gh_issue)

    # task create deleg
    task_create_deleg_parser = task_create_sub.add_parser("deleg", help="create DELEG_PLAN task for delegation")
    task_create_deleg_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_deleg_parser.add_argument("title", help="delegation title")
    # XOR: exactly one of plan or plan_ca_ref required
    deleg_plan_group = task_create_deleg_parser.add_mutually_exclusive_group(required=True)
    deleg_plan_group.add_argument("--plan", dest="plan", help="inline delegation plan (simple delegations)")
    deleg_plan_group.add_argument("--plan-ca-ref", dest="plan_ca_ref", help="path to deleg_plan.md CA (complex delegations)")
    task_create_deleg_parser.add_argument("--json", dest="json", action="store_true",
                                          help="output as JSON")
    task_create_deleg_parser.set_defaults(func=cmd_task_create_deleg)

    # task create task
    task_create_task_parser = task_create_sub.add_parser("task", help="create standalone task")
    task_create_task_parser.add_argument("--parent", default="000", help="parent task ID (default: 000)")
    task_create_task_parser.add_argument("title", help="task title")
    # XOR: exactly one of plan or plan_ca_ref required (uniform requirement)
    task_plan_group = task_create_task_parser.add_mutually_exclusive_group(required=True)
    task_plan_group.add_argument("--plan", dest="plan", help="inline plan description")
    task_plan_group.add_argument("--plan-ca-ref", dest="plan_ca_ref", help="path to plan CA")
    task_create_task_parser.add_argument("--json", dest="json", action="store_true",
                                          help="output as JSON")
    task_create_task_parser.set_defaults(func=cmd_task_create_task)

    # task archive
    task_archive_parser = task_sub.add_parser("archive", help="archive task to disk")
    task_archive_parser.add_argument("task_id", help="task ID to archive (e.g., #67 or 67)")
    task_archive_parser.add_argument("--no-cascade", dest="no_cascade", action="store_true",
                                     help="archive only this task, not children (default: cascade)")
    task_archive_parser.add_argument("--json", dest="json_output", action="store_true",
                                     help="output as JSON")
    task_archive_parser.set_defaults(func=cmd_task_archive)

    # task restore
    task_restore_parser = task_sub.add_parser("restore", help="restore task from archive")
    task_restore_parser.add_argument("archive_path_or_id", help="archive file path or original task ID")
    task_restore_parser.add_argument("--json", dest="json_output", action="store_true",
                                     help="output as JSON")
    task_restore_parser.set_defaults(func=cmd_task_restore)

    # task archived (subcommand group)
    task_archived_parser = task_sub.add_parser("archived", help="archived task operations")
    task_archived_sub = task_archived_parser.add_subparsers(dest="archived_cmd")

    # task archived list
    task_archived_list_parser = task_archived_sub.add_parser("list", help="list archived tasks")
    task_archived_list_parser.add_argument("--json", dest="json_output", action="store_true",
                                           help="output as JSON")
    task_archived_list_parser.set_defaults(func=cmd_task_archived_list)

    # task grant-update
    task_grant_update_parser = task_sub.add_parser("grant-update", help="grant permission to update task description")
    task_grant_update_parser.add_argument("task_id", help="task ID to grant update permission (e.g., #67 or 67)")
    task_grant_update_parser.add_argument("--field", "-f", help="specific MTMD field to grant modification for")
    task_grant_update_parser.add_argument("--value", "-v", help="expected new value (requires --field)")
    task_grant_update_parser.add_argument("--reason", "-r", default="", help="reason for granting")
    task_grant_update_parser.set_defaults(func=cmd_task_grant_update)

    # task grant-delete
    task_grant_delete_parser = task_sub.add_parser("grant-delete", help="grant permission to delete tasks")
    task_grant_delete_parser.add_argument("task_ids", nargs='+', help="task ID(s) to grant delete permission (e.g., #67 or 67, accepts multiple)")
    task_grant_delete_parser.add_argument("--reason", "-r", default="", help="reason for granting")
    task_grant_delete_parser.set_defaults(func=cmd_task_grant_delete)

    # task start
    task_start_parser = task_sub.add_parser("start", help="start work on task (→ in_progress)")
    task_start_parser.add_argument("task_id", help="task ID to start (e.g., #67 or 67)")
    task_start_parser.set_defaults(func=cmd_task_start)

    # task pause
    task_pause_parser = task_sub.add_parser("pause", help="pause work on task (→ pending)")
    task_pause_parser.add_argument("task_id", help="task ID to pause (e.g., #67 or 67)")
    task_pause_parser.set_defaults(func=cmd_task_pause)

    # task note - append note to updates
    task_note_parser = task_sub.add_parser("note", help="add a note to task (appends to updates with type='note')")
    task_note_parser.add_argument("task_id", help="task ID (e.g., #67 or 67)")
    task_note_parser.add_argument("message", help="note text")
    task_note_parser.set_defaults(func=cmd_task_note)

    # task complete
    task_complete_parser = task_sub.add_parser("complete", help="mark task complete with report")
    task_complete_parser.add_argument("task_id", help="task ID to complete (e.g., #67 or 67)")
    task_complete_parser.add_argument("--report", "-r",
                                      help="completion report (work done, difficulties, future work, git commit status)")
    task_complete_parser.add_argument("--commit", action="append", default=[],
                                      help="commit hash(es) that fix the issue (repeatable, required for GH_ISSUE)")
    task_complete_parser.add_argument("--verified",
                                      help="verification method description (required for GH_ISSUE)")
    task_complete_parser.set_defaults(func=cmd_task_complete)

    # task block - add blocking relationship
    task_block_parser = task_sub.add_parser("block", help="set task to block another task")
    task_block_parser.add_argument("task_id", help="task ID that will block (e.g., #60)")
    task_block_parser.add_argument("target_id", help="task ID to be blocked (e.g., #42)")
    task_block_parser.set_defaults(func=cmd_task_block)

    # task unblock - remove blocking relationship
    task_unblock_parser = task_sub.add_parser("unblock", help="remove blocking relationship")
    task_unblock_parser.add_argument("task_id", help="task ID that blocks (e.g., #60)")
    task_unblock_parser.add_argument("target_id", help="task ID to unblock (e.g., #42)")
    task_unblock_parser.set_defaults(func=cmd_task_unblock)

    # task blocked-by - add blocked-by relationship
    task_blocked_by_parser = task_sub.add_parser("blocked-by", help="set task as blocked by another task")
    task_blocked_by_parser.add_argument("task_id", help="task ID that is blocked (e.g., #60)")
    task_blocked_by_parser.add_argument("blocker_id", help="task ID that blocks (e.g., #26)")
    task_blocked_by_parser.set_defaults(func=cmd_task_blocked_by)

    # task unblocked-by - remove blocked-by relationship
    task_unblocked_by_parser = task_sub.add_parser("unblocked-by", help="remove blocked-by relationship")
    task_unblocked_by_parser.add_argument("task_id", help="task ID that was blocked (e.g., #60)")
    task_unblocked_by_parser.add_argument("blocker_id", help="task ID to remove as blocker (e.g., #26)")
    task_unblocked_by_parser.set_defaults(func=cmd_task_unblocked_by)

    # Proxy commands
    proxy_parser = sub.add_parser("proxy", help="API proxy for CC call interception")
    proxy_sub = proxy_parser.add_subparsers(dest="proxy_cmd")

    # proxy start
    proxy_start_parser = proxy_sub.add_parser("start", help="start proxy daemon")
    proxy_start_parser.add_argument("--daemon", "-d", action="store_true",
                                    help="run in background (daemonize)")
    proxy_start_parser.add_argument("--port", type=int, default=8019,
                                    help="port to listen on (default: 8019)")
    proxy_start_parser.set_defaults(func=cmd_proxy_start)

    # proxy stop
    proxy_sub.add_parser("stop", help="stop running proxy").set_defaults(func=cmd_proxy_stop)

    # proxy status
    proxy_status_parser = proxy_sub.add_parser("status", help="show proxy status")
    proxy_status_parser.add_argument("--json", dest="json_output", action="store_true",
                                     help="output as JSON")
    proxy_status_parser.set_defaults(func=cmd_proxy_status)

    # proxy stats
    proxy_sub.add_parser("stats", help="show aggregate token/cost statistics").set_defaults(func=cmd_proxy_stats)

    # proxy log
    proxy_log_parser = proxy_sub.add_parser("log", help="show recent API call events")
    proxy_log_parser.add_argument("--limit", "-n", type=int, default=10,
                                  help="number of recent events (default: 10)")
    proxy_log_parser.set_defaults(func=cmd_proxy_log)

    # Search service commands
    search_service_parser = sub.add_parser("search-service", help="search service daemon management")
    search_service_sub = search_service_parser.add_subparsers(dest="search_service_cmd")

    # search-service start
    start_parser = search_service_sub.add_parser("start", help="start search service daemon")
    start_parser.add_argument("--daemon", "-d", action="store_true",
                             help="run in background (daemonize)")
    start_parser.add_argument("--port", type=int, default=9001,
                             help="port to listen on (default: 9001)")
    start_parser.set_defaults(func=cmd_search_service_start)

    # search-service stop
    search_service_sub.add_parser("stop", help="stop running search service").set_defaults(func=cmd_search_service_stop)

    # search-service status
    status_parser = search_service_sub.add_parser("status", help="show search service status")
    status_parser.add_argument("--json", dest="json_output", action="store_true",
                              help="output as JSON")
    status_parser.set_defaults(func=cmd_search_service_status)

    # Transcripts command group
    transcripts_parser = sub.add_parser("transcripts", help="transcript forensics and search")
    transcripts_sub = transcripts_parser.add_subparsers(dest="transcripts_cmd")

    # transcripts search
    transcripts_search_parser = transcripts_sub.add_parser("search", help="search transcripts by breadcrumb")
    transcripts_search_parser.add_argument("breadcrumb", help="breadcrumb to search for (e.g., s_abc123/c_42/g_xyz/p_def456/t_123)")
    transcripts_search_parser.add_argument("--before", "-B", type=int, default=3,
                                           help="number of messages before target (default: 3)")
    transcripts_search_parser.add_argument("--after", "-A", type=int, default=3,
                                           help="number of messages after target (default: 3)")
    transcripts_search_parser.add_argument("--all", dest="search_all", action="store_true",
                                           help="search all transcripts (not just session from breadcrumb)")
    transcripts_search_parser.add_argument("--format", choices=["full", "compact", "json"], default="full",
                                           help="output format (default: full)")
    transcripts_search_parser.set_defaults(func=cmd_transcripts_search)

    # transcripts list
    transcripts_list_parser = transcripts_sub.add_parser("list", help="list all transcript files")
    transcripts_list_parser.add_argument("--json", dest="json_output", action="store_true",
                                         help="output as JSON")
    transcripts_list_parser.set_defaults(func=cmd_transcripts_list)

    # auto-restart: process supervisor
    ar_parser = sub.add_parser("auto-restart", help="auto-restarting process supervisor")
    ar_sub = ar_parser.add_subparsers(dest="ar_cmd")

    # auto-restart launch
    ar_launch = ar_sub.add_parser("launch", help="launch supervised process (direct mode by default)")
    ar_launch.add_argument("--name", "-n", default="", help="display name (default: command basename)")
    ar_launch.add_argument("--delay", "-d", type=int, default=5, help="restart delay in seconds (default: 5)")
    ar_launch.add_argument("--new-window", "-w", action="store_true", default=False,
                           help="launch in a new terminal window instead of current terminal")
    ar_launch.add_argument("--terminal", "-t", default="auto",
                           help="terminal app for --new-window mode (default: auto-detect)")
    ar_launch.add_argument("cmd", nargs=argparse.REMAINDER, help="command to supervise (after --)")
    ar_launch.set_defaults(func=lambda args: _cmd_ar_launch(args))

    # auto-restart list
    ar_list = ar_sub.add_parser("list", help="list managed processes (default: running only)")
    ar_list.add_argument("--all", "-a", action="store_true", dest="show_all",
                         help="show all including stopped/dead history")
    ar_list.set_defaults(func=lambda args: _cmd_ar_list(args))

    # auto-restart restart
    ar_restart = ar_sub.add_parser("restart", help="trigger restart (μC) for a supervised process")
    ar_restart.add_argument("pid", type=int, help="supervisor PID")
    ar_restart.set_defaults(func=lambda args: _cmd_ar_restart(args))

    # auto-restart disable
    ar_disable = ar_sub.add_parser("disable", help="disable auto-restart for a supervised process")
    ar_disable.add_argument("pid", type=int, help="supervisor PID")
    ar_disable.set_defaults(func=lambda args: _cmd_ar_disable(args))

    # auto-restart status
    ar_status = ar_sub.add_parser("status", help="detailed status of a supervised process")
    ar_status.add_argument("pid", type=int, help="supervisor PID")
    ar_status.set_defaults(func=lambda args: _cmd_ar_status(args))

    # auto-restart kill
    ar_kill = ar_sub.add_parser("kill", help="kill supervisor and child (nuclear option)")
    ar_kill.add_argument("pid", type=int, help="supervisor PID")
    ar_kill.set_defaults(func=lambda args: _cmd_ar_kill(args))

    return p

def main(argv=None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "cmd", None):
        # Capture argv before try block to avoid scope issues in exception handler
        argv_list = argv if argv else sys.argv[1:]
        # Log CLI command invocation for forensic reconstruction
        try:
            session_id = get_current_session_id()
            cmd = getattr(args, "cmd", "unknown")
            subcmd = getattr(args, "subcmd", None)
            command_str = f"{cmd} {subcmd}" if subcmd else cmd
            append_event(
                event="cli_command_invoked",
                data={
                    "session_id": session_id,
                    "command": command_str,
                    "argv": argv_list
                }
            )
        except Exception as e:
            # Log error but don't break CLI functionality (use print, not sys.stderr)
            print(f"🏗️ MACF | ❌ CLI event logging error: {e}")
        exit(args.func(args))
    parser.print_help()

if __name__ == "__main__":
    main()
