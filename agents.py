"""Agent trigger — writes to queue files picked up by visible worker terminals."""

import json
import logging
import uuid
from pathlib import Path

from queue_utils import append_queue_line

log = logging.getLogger(__name__)


class AgentTrigger:
    def __init__(self, registry, data_dir: str = "./data"):
        self._registry = registry
        self._data_dir = Path(data_dir)

    def is_available(self, name: str) -> bool:
        return self._registry.is_registered(name)

    def get_status(self) -> dict:
        from mcp_bridge import is_online, is_active, get_role
        instances = self._registry.get_all()
        return {
            name: {
                "available": is_online(name),
                "busy": is_active(name),
                "label": info["label"],
                "color": info["color"],
                "role": get_role(name),
            }
            for name, info in instances.items()
        }

    async def trigger(self, agent_name: str, message: str = "", channel: str = "general",
                      job_id: int | None = None, **kwargs):
        """Write to the agent's queue file. The worker terminal picks it up."""
        queue_file = self._data_dir / f"{agent_name}_queue.jsonl"
        self._data_dir.mkdir(parents=True, exist_ok=True)

        import time
        trigger_id = uuid.uuid4().hex
        entry = {
            "trigger_id": trigger_id,
            "sender": message.split(":")[0].strip() if ":" in message else "?",
            "text": message,
            "time": time.strftime("%H:%M:%S"),
            "channel": channel,
        }
        custom_prompt = kwargs.get("prompt", "")
        if isinstance(custom_prompt, str) and custom_prompt.strip():
            entry["prompt"] = custom_prompt.strip()
        if job_id is not None:
            entry["job_id"] = job_id

        append_queue_line(queue_file, json.dumps(entry))

        log.info("Queued trigger_id=%s @%s (ch=%s, job=%s): %s",
                 trigger_id, agent_name, channel, job_id, message[:80])

    def trigger_sync(self, agent_name: str, message: str = "", channel: str = "general",
                     job_id: int | None = None, **kwargs):
        """Synchronous version of trigger — writes to queue file without async."""
        queue_file = self._data_dir / f"{agent_name}_queue.jsonl"
        self._data_dir.mkdir(parents=True, exist_ok=True)

        import time
        trigger_id = uuid.uuid4().hex
        entry = {
            "trigger_id": trigger_id,
            "sender": message.split(":")[0].strip() if ":" in message else "?",
            "text": message,
            "time": time.strftime("%H:%M:%S"),
            "channel": channel,
        }
        custom_prompt = kwargs.get("prompt", "")
        if isinstance(custom_prompt, str) and custom_prompt.strip():
            entry["prompt"] = custom_prompt.strip()
        if job_id is not None:
            entry["job_id"] = job_id

        append_queue_line(queue_file, json.dumps(entry))

        log.info("Queued trigger_id=%s @%s (ch=%s, job=%s): %s",
                 trigger_id, agent_name, channel, job_id, message[:80])
