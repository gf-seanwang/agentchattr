"""Telegram bridge for agentchattr — runs as background thread inside the server."""

import json
import logging
import os
import re
import threading
import time
import urllib.request
import urllib.error
from pathlib import Path

log = logging.getLogger("tg_bridge")

ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / "tg_state.json"


# --- Config ---

def load_config() -> dict | None:
    config_path = ROOT / "tg_config.toml"
    if not config_path.exists():
        return None
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def check_config() -> list[str]:
    """Validate config file, return list of errors (empty = OK)."""
    errors = []
    try:
        cfg = load_config()
    except Exception as e:
        return [f"tg_config.toml parse error: {e}"]
    if cfg is None:
        return ["tg_config.toml not found"]
    token = cfg.get("bot_token", "").strip()
    if not token or token.startswith("123456"):
        errors.append("bot_token is not set or is still the example value")
    channel = cfg.get("channel", "").strip()
    if not channel:
        errors.append("channel is not set")
    allowed = cfg.get("allowed_users", [])
    if not allowed:
        errors.append("allowed_users is not set (list of Telegram usernames or user IDs)")
    return errors


def validate_runtime(cfg: dict, room_settings: dict) -> tuple[list[str], list[str]]:
    """Validate config against runtime state. Returns (errors, warnings)."""
    errors = []
    warnings = []
    channel = cfg.get("channel", "")
    channels = room_settings.get("channels", ["general"])
    if channel not in channels:
        errors.append(f"channel '{channel}' does not exist in agentchattr (available: {', '.join(channels)})")
    elif channel != "general":
        ca = room_settings.get("channel_agents", {})
        if channel not in ca:
            warnings.append(f"channel '{channel}' has no channel_agents configured — agents may not be restricted")
    return errors, warnings


# --- State persistence ---

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text("utf-8"))
        except Exception:
            pass
    return {"telegram_update_offset": 0, "cursor": -1}


def _save_state(state: dict):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), "utf-8")
    os.replace(str(tmp), str(STATE_FILE))


# --- Helpers ---

def _safe_tg_sender(user: dict) -> str:
    """Sanitize Telegram user identity for use as agentchattr sender."""
    raw = user.get("username") or user.get("first_name") or f"user_{user.get('id', 'unknown')}"
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", raw).strip("_")
    return (safe[:32] or f"user_{user.get('id', 'unknown')}")


# --- Telegram API ---

class _TelegramBot:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"

    def _call(self, method: str, data: dict | None = None) -> dict:
        url = f"{self.base}/{method}"
        if data:
            body = json.dumps(data).encode("utf-8")
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        else:
            req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            log.error("TG API %s error %s: %s", method, e.code, err_body)
            return {"ok": False}
        except Exception as e:
            log.error("TG API %s failed: %s", method, e)
            return {"ok": False}

    def get_updates(self, offset: int = 0, timeout: int = 1) -> list[dict]:
        params: dict = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset:
            params["offset"] = offset
        result = self._call("getUpdates", params)
        return result.get("result", []) if result.get("ok") else []

    def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> dict:
        data: dict = {"chat_id": chat_id, "text": text}
        if reply_markup:
            data["reply_markup"] = reply_markup
        return self._call("sendMessage", data)

    def set_reply_keyboard(self, chat_id: str, buttons: list[list[str]], text: str = "⌨️") -> dict:
        keyboard = {"keyboard": [[{"text": b} for b in row] for row in buttons], "resize_keyboard": True}
        return self.send_message(chat_id, text, reply_markup=keyboard)


# --- Bridge ---

class TGBridge:
    def __init__(self, cfg: dict, store, registry, room_settings: dict):
        self.bot = _TelegramBot(cfg["bot_token"])
        self.cfg = cfg
        self.channel = cfg["channel"]
        self.poll_interval = cfg.get("poll_interval_seconds", 1.5)
        self.allowed_users: set[str] = {str(u) for u in cfg.get("allowed_users", [])}
        self.store = store
        self.registry = registry
        self.room_settings = room_settings
        self.state = _load_state()
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_keyboard: dict[str, list] = {}
        self._last_keyboard_refresh = 0.0

    @property
    def running(self) -> bool:
        return self._running and self._thread is not None and self._thread.is_alive()

    def start(self):
        with self._lock:
            if self.running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._loop, daemon=True, name="tg-bridge")
            self._thread.start()
        log.info("TG Bridge started — #%s", self.channel)

    def stop(self):
        with self._lock:
            self._running = False
            t = self._thread
            self._thread = None
        if t:
            t.join(timeout=35)
        log.info("TG Bridge stopped")

    # --- Chat binding ---

    def _configured_chat_id(self) -> str:
        return str(self.cfg.get("chat_id", "")).strip()

    def _bound_chat_id(self) -> str:
        return self._configured_chat_id() or str(self.state.get("bound_chat_id", "")).strip()

    def _resolve_or_bind_chat(self, chat_id: str) -> bool:
        """Check if chat_id is the bound chat. First allowed message binds if not configured."""
        configured = self._configured_chat_id()
        if configured:
            return chat_id == configured
        bound = str(self.state.get("bound_chat_id", "")).strip()
        if bound:
            return chat_id == bound
        self.state["bound_chat_id"] = chat_id
        _save_state(self.state)
        log.info("Auto-bound to chat %s", chat_id)
        return True

    def _unbind_chat(self, chat_id: str) -> str:
        """Unbind current chat. Returns message for user."""
        if self._configured_chat_id():
            return "chat_id is configured in tg_config.toml and cannot be unbound from Telegram."
        bound = str(self.state.get("bound_chat_id", "")).strip()
        if not bound:
            return "No chat is currently bound."
        if chat_id != bound:
            return "You can only unbind from the currently bound chat."
        del self.state["bound_chat_id"]
        _save_state(self.state)
        log.info("Chat unbound by user")
        return "Unbound. Next message from an allowed user will bind a new chat."

    # --- Helpers ---

    def _get_agents_in_channel(self) -> list[str]:
        agents = self.room_settings.get("channel_agents", {}).get(self.channel, [])
        if not agents and self.channel == "general" and self.registry:
            return self.registry.get_active_names()
        return agents

    def _get_agent_label(self, name: str) -> str:
        if self.registry:
            inst = self.registry.get_instance(name)
            if inst:
                return inst.get("label", name)
        return name

    def _known_agents(self) -> set[str]:
        agents = set()
        if self.registry:
            agents.update(self.registry.get_all_names())
            agents.update(self.registry.get_bases().keys())
        return agents

    # --- Commands ---

    def _handle_command(self, text: str, chat_id: str) -> bool | tuple:
        parts = text.strip().split(None, 1)
        cmd = parts[0].lower().split("@")[0]
        rest = parts[1] if len(parts) > 1 else ""

        if cmd == "/start":
            agents = self._get_agents_in_channel()
            agent_list = "\n".join(f"  @{a}" for a in agents) if agents else "  (none)"
            self.bot.send_message(chat_id,
                f"agentchattr — #{self.channel}\n\n"
                f"Agents:\n{agent_list}\n\n"
                f"/agents — status\n"
                f"/all <msg> — mention all\n"
                f"/unbind — reset chat binding\n\n"
                f"Or type @agent-name <message>")
            self._refresh_keyboard(chat_id, force=True)
            return True

        if cmd in ("/agents", "/status"):
            agents = self._get_agents_in_channel()
            if not agents:
                self.bot.send_message(chat_id, f"#{self.channel} has no agents.")
                return True
            lines = [f"#{self.channel}:"]
            for name in agents:
                available = False
                if self.registry:
                    inst = self.registry.get_instance(name)
                    if inst and inst.get("state") == "active":
                        available = True
                icon = "🟢" if available else "⚫"
                label = self._get_agent_label(name)
                lines.append(f"  {icon} {label} ({name})")
            self.bot.send_message(chat_id, "\n".join(lines))
            self._refresh_keyboard(chat_id)
            return True

        if cmd == "/all":
            if not rest:
                self.bot.send_message(chat_id, "Usage: /all <message>")
                return True
            agents = self._get_agents_in_channel()
            if not agents:
                self.bot.send_message(chat_id, f"No agents in #{self.channel}.")
                return True
            mentions = " ".join(f"@{a}" for a in agents)
            return (False, f"{mentions} {rest}")

        if cmd == "/unbind":
            result = self._unbind_chat(chat_id)
            self.bot.send_message(chat_id, result)
            return True

        return False

    # --- Keyboard ---

    def _refresh_keyboard(self, chat_id: str, force: bool = False):
        agents = self._get_agents_in_channel()
        if not force and agents == self._last_keyboard.get(chat_id):
            return
        rows = []
        row = []
        for name in agents:
            row.append(f"@{name}")
            if len(row) >= 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append(["/agents", "/status", "/all"])
        self.bot.set_reply_keyboard(chat_id, rows, text=f"#{self.channel}")
        self._last_keyboard[chat_id] = list(agents)

    # --- Polling ---

    def _poll_telegram(self):
        offset = self.state.get("telegram_update_offset", 0)
        updates = self.bot.get_updates(offset=offset)
        for update in updates:
            self.state["telegram_update_offset"] = update.get("update_id", 0) + 1

            msg = update.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text = msg.get("text", "").strip()
            if not chat_id or not text:
                continue

            user = msg.get("from", {})
            user_id = str(user.get("id", ""))
            tg_username = user.get("username", "")

            # Auth: allowed_users check (username or user_id only, not first_name)
            if self.allowed_users and tg_username not in self.allowed_users and user_id not in self.allowed_users:
                log.debug("Rejected unauthorized user: %s (%s)", tg_username or user.get("first_name", "?"), user_id)
                continue

            # Chat binding: only accept from bound/configured chat
            if not self._resolve_or_bind_chat(chat_id):
                log.debug("Rejected message from non-bound chat: %s", chat_id)
                continue

            safe_sender = _safe_tg_sender(user)
            log.info("TG [#%s] %s: %s", self.channel, safe_sender, text[:80])

            if text.startswith("/"):
                result = self._handle_command(text, chat_id)
                if result is True:
                    continue
                if isinstance(result, tuple):
                    _, text = result

            self.store.add(f"tg:{safe_sender}", text, channel=self.channel,
                           metadata={
                               "source": "telegram",
                               "tg_chat_id": chat_id,
                               "tg_user_id": user_id,
                               "tg_username": tg_username,
                               "tg_first_name": user.get("first_name", ""),
                           })

        if updates:
            _save_state(self.state)

    def _poll_outbound(self):
        bound = self._bound_chat_id()
        if not bound:
            return
        since_id = self.state.get("cursor", 0)
        msgs = self.store.get_since(since_id, channel=self.channel)
        known = self._known_agents()
        delivered = self.state.setdefault("delivered", {})
        safe_cursor = since_id
        any_failure = False

        for m in msgs:
            mid = m.get("id", 0)
            mid_str = str(mid)
            sender = m.get("sender", "")
            if sender.startswith("tg:") or m.get("type") in ("system", "join", "leave") or sender not in known:
                if mid > safe_cursor:
                    safe_cursor = mid
                continue

            label = self._get_agent_label(sender)
            tg_text = self._format_for_telegram(f"[{label}]\n{m.get('text', '')}")

            if delivered.get(mid_str) == "sent":
                if mid > safe_cursor:
                    safe_cursor = mid
                continue

            if self.bot.send_message(bound, tg_text).get("ok", False):
                delivered[mid_str] = "sent"
                if mid > safe_cursor:
                    safe_cursor = mid
            else:
                delivered[mid_str] = "pending"
                any_failure = True
                break

        if safe_cursor > since_id:
            self.state["cursor"] = safe_cursor
            old_keys = [k for k in list(delivered.keys()) if int(k) <= safe_cursor]
            for k in old_keys:
                del delivered[k]
            _save_state(self.state)
        elif any_failure:
            _save_state(self.state)

    _TRUNCATION_NOTICE = "\n\n[...truncated, see web UI for full message]"

    def _format_for_telegram(self, text: str, max_len: int = 4096) -> str:
        if len(text) <= max_len:
            return text
        budget = max_len - len(self._TRUNCATION_NOTICE)
        return text[:budget].rstrip() + self._TRUNCATION_NOTICE

    def _periodic_keyboard_refresh(self):
        now = time.time()
        if now - self._last_keyboard_refresh < 30:
            return
        self._last_keyboard_refresh = now
        bound = self._bound_chat_id()
        if bound:
            self._refresh_keyboard(bound)

    # --- Main loop ---

    def _loop(self):
        if self.state.get("cursor", -1) < 0:
            msgs = self.store.get_since(-1, channel=self.channel)
            if msgs:
                self.state["cursor"] = max(m.get("id", 0) for m in msgs)
            else:
                self.state["cursor"] = 0
            _save_state(self.state)

        while self._running:
            try:
                self._poll_telegram()
                self._poll_outbound()
                self._periodic_keyboard_refresh()
            except Exception as e:
                log.error("Bridge loop error: %s", e, exc_info=True)
            time.sleep(self.poll_interval)
