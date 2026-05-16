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
        params: dict = {"timeout": timeout, "allowed_updates": ["message", "callback_query"]}
        if offset:
            params["offset"] = offset
        result = self._call("getUpdates", params)
        return result.get("result", []) if result.get("ok") else []

    def answer_callback_query(self, callback_query_id: str, text: str = "") -> dict:
        data: dict = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text
        return self._call("answerCallbackQuery", data)

    def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> dict:
        data: dict = {"chat_id": chat_id, "text": text}
        if reply_markup:
            data["reply_markup"] = reply_markup
        return self._call("sendMessage", data)

    def edit_message_reply_markup(self, chat_id: str, message_id: int, reply_markup: dict | None = None) -> dict:
        data: dict = {"chat_id": chat_id, "message_id": message_id}
        if reply_markup:
            data["reply_markup"] = reply_markup
        else:
            data["reply_markup"] = {"inline_keyboard": []}
        return self._call("editMessageReplyMarkup", data)

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
        self._pending_mention: dict[str, str] = {}  # chat_id -> "@agent-name"

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running and self._thread is not None and self._thread.is_alive()

    @property
    def stopping(self) -> bool:
        with self._lock:
            return not self._running and self._thread is not None and self._thread.is_alive()

    def start(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._running = True
            self._thread = threading.Thread(target=self._loop, daemon=True, name="tg-bridge")
            self._thread.start()
        log.info("TG Bridge started — #%s", self.channel)

    def stop(self):
        with self._lock:
            self._running = False
            t = self._thread
        if t:
            t.join(timeout=35)
        with self._lock:
            self._thread = None
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

        if cmd == "/cancel":
            if chat_id in self._pending_mention:
                cleared = self._pending_mention.pop(chat_id)
                self.bot.send_message(chat_id, f"✗ {cleared} 已��消")
            else:
                self.bot.send_message(chat_id, "沒有待送出的 mention")
            return True

        if cmd == "/unbind":
            result = self._unbind_chat(chat_id)
            self.bot.send_message(chat_id, result)
            return True

        # /agent-name [msg] — convert to @agent-name mention
        agent_name = cmd.lstrip("/")
        agents = self._get_agents_in_channel()
        if agent_name in agents:
            if rest:
                return (False, f"@{agent_name} {rest}")
            self._pending_mention[chat_id] = f"@{agent_name}"
            self.bot.send_message(chat_id, f"✓ @{agent_name} — 請輸入訊息")
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
            row.append(f"/{name}")
            if len(row) >= 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append(["/all", "/cancel", "/agents"])
        self.bot.set_reply_keyboard(chat_id, rows, text=f"#{self.channel}")
        self._last_keyboard[chat_id] = list(agents)

    # --- Polling ---

    def _handle_callback_query(self, update: dict):
        """Handle inline keyboard button presses (decision choices)."""
        cb = update.get("callback_query", {})
        cb_id = cb.get("id", "")
        data = cb.get("data", "")

        # Auth: validate user and chat binding
        cb_user = cb.get("from", {})
        cb_user_id = str(cb_user.get("id", ""))
        cb_username = cb_user.get("username", "")
        if self.allowed_users and cb_username not in self.allowed_users and cb_user_id not in self.allowed_users:
            self.bot.answer_callback_query(cb_id, "Unauthorized")
            return
        cb_chat_id = str(cb.get("message", {}).get("chat", {}).get("id", ""))
        bound = self._bound_chat_id()
        if not bound or cb_chat_id != bound:
            self.bot.answer_callback_query(cb_id, "No active chat binding")
            return

        if not data.startswith("decide:"):
            self.bot.answer_callback_query(cb_id, "Unknown action")
            return
        parts = data.split(":")
        if len(parts) < 3:
            self.bot.answer_callback_query(cb_id, "Invalid data")
            return
        try:
            msg_id = int(parts[1])
            choice_idx = int(parts[2])
        except ValueError:
            self.bot.answer_callback_query(cb_id, "Invalid data")
            return
        # Check and claim under lock — no TG API calls inside lock
        error_msg = None
        choice = None
        sender = ""
        channel = "general"
        msg = None
        meta = {}
        try:
            with self.store._lock:
                for m in self.store._messages:
                    if m["id"] == msg_id:
                        msg = m
                        break
                if not msg or msg.get("type") != "decision":
                    error_msg = "Message not found"
                else:
                    meta = msg.get("metadata") or {}
                    if meta.get("resolved"):
                        error_msg = f"Already chosen: {meta.get('chosen', '')}"
                    else:
                        choices = meta.get("choices", [])
                        if choice_idx < 0 or choice_idx >= len(choices):
                            error_msg = "Invalid choice"
                        else:
                            choice = choices[choice_idx]
                            sender = msg.get("sender", "")
                            channel = msg.get("channel", "general")
                            meta["resolved"] = True
                            meta["chosen"] = choice
                            msg["metadata"] = meta
                            self.store._rewrite()
        except Exception as e:
            log.error("Failed to resolve decision: %s", e)
            self.bot.answer_callback_query(cb_id, "Error")
            return

        if error_msg:
            self.bot.answer_callback_query(cb_id, error_msg)
            return

        # Add reply outside lock
        username = self.room_settings.get("username", "user")
        reply_text = f"@{sender} {choice}" if sender else choice
        try:
            self.store.add(username, reply_text, reply_to=msg_id, channel=channel)
        except Exception as e:
            log.error("Failed to add decision reply, rolling back: %s", e)
            with self.store._lock:
                meta["resolved"] = False
                meta.pop("chosen", None)
                msg["metadata"] = meta
                self.store._rewrite()
            self.bot.answer_callback_query(cb_id, "Error saving reply")
            return

        # Remove inline buttons from TG message
        decision_tg = self.state.get("decision_tg_msgs", {})
        entry = decision_tg.get(str(msg_id))
        if entry and isinstance(entry, dict):
            tg_msg_id = entry.get("tg_msg_id")
            orig_chat = entry.get("chat_id", "")
            if tg_msg_id and orig_chat:
                if self.bot.edit_message_reply_markup(orig_chat, tg_msg_id).get("ok", False):
                    decision_tg.pop(str(msg_id), None)
                    _save_state(self.state)

        # Broadcast update to web clients
        try:
            import asyncio as _aio
            from app import _broadcast, _event_loop
            updated = self.store.get_by_id(msg_id)
            if updated and _event_loop:
                _aio.run_coroutine_threadsafe(
                    _broadcast(json.dumps({"type": "message_update", "message": updated})),
                    _event_loop,
                )
        except Exception:
            pass
        self.bot.answer_callback_query(cb_id, f"✓ {choice}")

    def _poll_telegram(self):
        offset = self.state.get("telegram_update_offset", 0)
        updates = self.bot.get_updates(offset=offset)
        for update in updates:
            uid = update.get("update_id", 0)

            # Handle inline button callbacks
            if "callback_query" in update:
                self._handle_callback_query(update)
                self.state["telegram_update_offset"] = uid + 1
                continue

            msg = update.get("message", {})
            chat_id = str(msg.get("chat", {}).get("id", ""))
            text = msg.get("text", "").strip()
            if not chat_id or not text:
                self.state["telegram_update_offset"] = uid + 1
                continue

            user = msg.get("from", {})
            user_id = str(user.get("id", ""))
            tg_username = user.get("username", "")

            # Auth: allowed_users check (username or user_id only, not first_name)
            if self.allowed_users and tg_username not in self.allowed_users and user_id not in self.allowed_users:
                log.debug("Rejected unauthorized user: %s (%s)", tg_username or user.get("first_name", "?"), user_id)
                self.state["telegram_update_offset"] = uid + 1
                continue

            # Chat binding: only accept from bound/configured chat
            if not self._resolve_or_bind_chat(chat_id):
                log.debug("Rejected message from non-bound chat: %s", chat_id)
                self.state["telegram_update_offset"] = uid + 1
                continue

            safe_sender = _safe_tg_sender(user)
            log.info("TG [#%s] %s: %s", self.channel, safe_sender, text[:80])

            if text.startswith("/"):
                result = self._handle_command(text, chat_id)
                if result is True:
                    self.state["telegram_update_offset"] = uid + 1
                    continue
                if isinstance(result, tuple):
                    _, text = result
            elif chat_id in self._pending_mention:
                text = f"{self._pending_mention.pop(chat_id)} {text}"

            try:
                display_sender = self.room_settings.get("username", f"tg:{safe_sender}")
                self.store.add(display_sender, text, channel=self.channel,
                               metadata={
                                   "source": "telegram",
                                   "tg_chat_id": chat_id,
                                   "tg_user_id": user_id,
                                   "tg_username": tg_username,
                                   "tg_first_name": user.get("first_name", ""),
                               })
                self.state["telegram_update_offset"] = uid + 1
            except Exception as e:
                log.error("Failed to persist TG message, will retry: %s", e)
                break

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
            if not self._running:
                break
            mid = m.get("id", 0)
            mid_str = str(mid)
            sender = m.get("sender", "")
            meta = m.get("metadata") or {}
            is_from_tg = sender.startswith("tg:") or meta.get("source") == "telegram"
            if is_from_tg or m.get("type") in ("system", "join", "leave"):
                if mid > safe_cursor:
                    safe_cursor = mid
                continue

            label = self._get_agent_label(sender)
            tg_text = self._format_for_telegram(f"[{label}]\n{m.get('text', '')}")

            # Build inline keyboard for decision messages
            reply_markup = None
            choices = meta.get("choices", [])
            if m.get("type") == "decision" and choices and not meta.get("resolved"):
                buttons = [[{"text": c, "callback_data": f"decide:{mid}:{i}"}] for i, c in enumerate(choices)]
                reply_markup = {"inline_keyboard": buttons}

            if delivered.get(mid_str) == "sent":
                if mid > safe_cursor:
                    safe_cursor = mid
                continue

            send_result = self.bot.send_message(bound, tg_text, reply_markup=reply_markup)
            if send_result.get("ok", False):
                delivered[mid_str] = "sent"
                if reply_markup:
                    tg_msg_id = send_result.get("result", {}).get("message_id")
                    if tg_msg_id:
                        decision_tg = self.state.setdefault("decision_tg_msgs", {})
                        decision_tg[mid_str] = {"tg_msg_id": tg_msg_id, "chat_id": bound}
                if mid > safe_cursor:
                    safe_cursor = mid
            else:
                delivered[mid_str] = "pending"
                any_failure = True
                break

        # Clean up decisions resolved from web UI
        state_dirty = False
        decision_tg = self.state.get("decision_tg_msgs", {})
        if decision_tg:
            for dmid_str in list(decision_tg.keys()):
                dmsg = self.store.get_by_id(int(dmid_str))
                if dmsg and (dmsg.get("metadata") or {}).get("resolved"):
                    entry = decision_tg[dmid_str]
                    if isinstance(entry, dict):
                        tg_msg_id = entry.get("tg_msg_id")
                        orig_chat = entry.get("chat_id", "")
                    else:
                        tg_msg_id = entry
                        orig_chat = bound
                    if tg_msg_id and orig_chat:
                        if self.bot.edit_message_reply_markup(orig_chat, tg_msg_id).get("ok", False):
                            decision_tg.pop(dmid_str)
                            state_dirty = True

        if safe_cursor > since_id:
            self.state["cursor"] = safe_cursor
            old_keys = [k for k in list(delivered.keys()) if int(k) <= safe_cursor]
            for k in old_keys:
                del delivered[k]
            state_dirty = True
        if state_dirty or any_failure:
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

    # --- Startup ---

    def _ensure_cursor_channel(self):
        stored = self.state.get("cursor_channel")
        if stored == self.channel:
            return
        self.state["cursor"] = -1
        self.state["cursor_channel"] = self.channel
        self.state.pop("delivered", None)
        _save_state(self.state)
        log.info("Cursor channel changed from %r to %r; cursor reset", stored, self.channel)

    def _ensure_outbound_cursor(self):
        if self.state.get("cursor", -1) >= 0:
            return
        msgs = self.store.get_since(-1, channel=self.channel)
        self.state["cursor"] = max((m.get("id", 0) for m in msgs), default=0)
        _save_state(self.state)

    def _drain_telegram_backlog(self):
        if self.state.get("telegram_backlog_drained"):
            return
        updates = self.bot.get_updates(offset=-1, timeout=0)
        if updates:
            latest = max(u.get("update_id", 0) for u in updates)
            self.state["telegram_update_offset"] = latest + 1
            log.info("Drained pending TG updates, starting from offset %d", latest + 1)
        self.state["telegram_backlog_drained"] = True
        _save_state(self.state)

    # --- Main loop ---

    def _loop(self):
        self._ensure_cursor_channel()
        self._ensure_outbound_cursor()
        self._drain_telegram_backlog()

        while self._running:
            try:
                self._poll_telegram()
                self._poll_outbound()
                self._periodic_keyboard_refresh()
            except Exception as e:
                log.error("Bridge loop error: %s", e, exc_info=True)
            time.sleep(self.poll_interval)
