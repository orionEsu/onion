"""Shared utilities to avoid circular imports."""

import logging
import time as time_mod

logger = logging.getLogger(__name__)


def store_undo(context, action_type: str, task_id: int, previous_state: dict):
    """Store the last destructive action for potential undo.

    action_type: "done", "delete", "cancel", "edit"
    previous_state: dict snapshot of the task row before the action

    Note: Only the most recent action can be undone. A new action overwrites the previous one.
    """
    existing = context.application.bot_data.get("last_undo")
    if existing:
        logger.debug("Undo overwritten: previous %s on task #%s replaced by %s on task #%s",
                      existing["type"], existing["task_id"], action_type, task_id)
    context.application.bot_data["last_undo"] = {
        "type": action_type,
        "task_id": task_id,
        "previous_state": previous_state,
        "timestamp": time_mod.time(),
    }


def task_to_dict(task) -> dict:
    """Convert a sqlite3.Row to a plain dict for undo storage."""
    return {key: task[key] for key in task.keys()}
