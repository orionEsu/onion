"""Tests for inline keyboard callback handlers."""

import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from bot import database as db
from bot.config import TIMEZONE, AUTHORIZED_USER_ID
from bot.callbacks import handle_callback


def _set_callback(mock_update, data: str):
    """Configure mock_update for a callback query."""
    mock_update.effective_user.id = AUTHORIZED_USER_ID
    mock_update.callback_query.data = data


# ── Review callbacks ──────────────────────────────────────────────


class TestReviewCallbacks:
    @pytest.mark.asyncio
    async def test_review_done(self, mock_update, mock_context):
        tid = db.add_task("Review task", "2026-03-10", None)
        _set_callback(mock_update, f"review_done_{tid}")
        await handle_callback(mock_update, mock_context)
        assert db.get_task(tid)["status"] == "done"

    @pytest.mark.asyncio
    async def test_review_done_recurring_creates_next(self, mock_update, mock_context):
        tid = db.add_task("Recurring", "2026-03-10", None, recurrence_rule="daily")
        _set_callback(mock_update, f"review_done_{tid}")
        await handle_callback(mock_update, mock_context)
        assert db.get_task(tid)["status"] == "done"
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Next" in text

    @pytest.mark.asyncio
    async def test_review_undone_shows_options(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        _set_callback(mock_update, f"review_undone_{tid}")
        await handle_callback(mock_update, mock_context)
        call = mock_update.callback_query.edit_message_text
        assert "not done" in call.call_args.args[0].lower() or "Carry" in str(call.call_args)

    @pytest.mark.asyncio
    async def test_carry_tomorrow(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        _set_callback(mock_update, f"carry_tomorrow_{tid}")
        await handle_callback(mock_update, mock_context)
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        assert db.get_task(tid)["due_date"] == tomorrow

    @pytest.mark.asyncio
    async def test_carry_drop(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        _set_callback(mock_update, f"carry_drop_{tid}")
        await handle_callback(mock_update, mock_context)
        assert db.get_task(tid)["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_carry_pick_stores_awaiting(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_context.user_data = {}
        _set_callback(mock_update, f"carry_pick_{tid}")
        await handle_callback(mock_update, mock_context)
        assert mock_context.user_data["awaiting_carry_date"] == tid


# ── Task confirmation callbacks ───────────────────────────────────


class TestConfirmCallbacks:
    @pytest.mark.asyncio
    async def test_confirm_add(self, mock_update, mock_context):
        from bot.models import ParsedTask
        parsed = ParsedTask(
            description="New task", due_date="2026-03-15", due_time="14:00",
            label_names=["Work"],
        )
        mock_context.user_data = {"pending_task": parsed}
        _set_callback(mock_update, "confirm_add")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Added" in text or "New task" in text

    @pytest.mark.asyncio
    async def test_cancel_add(self, mock_update, mock_context):
        from bot.models import ParsedTask
        parsed = ParsedTask(description="Task", due_date="2026-03-15")
        mock_context.user_data = {"pending_task": parsed}
        _set_callback(mock_update, "cancel_add")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Cancelled" in text
        assert "pending_task" not in mock_context.user_data


# ── Label toggle callbacks ────────────────────────────────────────


class TestLabelCallbacks:
    @pytest.mark.asyncio
    async def test_label_toggle_on(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        work = db.get_label_by_name("Work")
        _set_callback(mock_update, f"label_toggle_{tid}_{work['id']}")
        await handle_callback(mock_update, mock_context)
        labels = db.get_labels_for_task(tid)
        assert any(l["name"] == "Work" for l in labels)

    @pytest.mark.asyncio
    async def test_label_toggle_off(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        work = db.get_label_by_name("Work")
        db.add_task_label(tid, work["id"])
        _set_callback(mock_update, f"label_toggle_{tid}_{work['id']}")
        await handle_callback(mock_update, mock_context)
        labels = db.get_labels_for_task(tid)
        assert not any(l["name"] == "Work" for l in labels)

    @pytest.mark.asyncio
    async def test_label_done(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        work = db.get_label_by_name("Work")
        db.add_task_label(tid, work["id"])
        _set_callback(mock_update, f"label_done_{tid}")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Work" in text

    @pytest.mark.asyncio
    async def test_label_skip(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        _set_callback(mock_update, f"label_skip_{tid}")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Skipped" in text


# ── Snooze callbacks ──────────────────────────────────────────────


class TestSnoozeCallbacks:
    @pytest.mark.asyncio
    async def test_snooze_tomorrow(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", "14:00")
        _set_callback(mock_update, f"snooze_tomorrow_{tid}")
        await handle_callback(mock_update, mock_context)
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        assert db.get_task(tid)["due_date"] == tomorrow

    @pytest.mark.asyncio
    async def test_snooze_1h(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tid = db.add_task("Task", today, "14:00")
        _set_callback(mock_update, f"snooze_1h_{tid}")
        await handle_callback(mock_update, mock_context)
        task = db.get_task(tid)
        assert task["due_time"] == "15:00"

    @pytest.mark.asyncio
    async def test_snooze_3h(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tid = db.add_task("Task", today, "14:00")
        _set_callback(mock_update, f"snooze_3h_{tid}")
        await handle_callback(mock_update, mock_context)
        task = db.get_task(tid)
        assert task["due_time"] == "17:00"


# ── Morning done callback ────────────────────────────────────────


class TestMorningDoneCallback:
    @pytest.mark.asyncio
    async def test_morning_done(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tid = db.add_task("Morning task", today, None)
        mock_context.application.bot_data["morning_prompt_active"] = True
        mock_context.application.bot_data["morning_prompt_tasks"] = [tid]
        _set_callback(mock_update, "morning_done")
        await handle_callback(mock_update, mock_context)
        assert mock_context.application.bot_data["morning_prompt_active"] is False
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Morning Planning Done" in text


# ── Clear callbacks ───────────────────────────────────────────────


class TestClearCallbacks:
    @pytest.mark.asyncio
    async def test_clear_confirm(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        db.add_task("Task", today, None)
        _set_callback(mock_update, "clear_confirm_today")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Cleared" in text

    @pytest.mark.asyncio
    async def test_clear_cancel(self, mock_update, mock_context):
        _set_callback(mock_update, "clear_cancel")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "cancelled" in text.lower()

    @pytest.mark.asyncio
    async def test_clear_invalid_scope_ignored(self, mock_update, mock_context):
        _set_callback(mock_update, "clear_confirm_bogus")
        await handle_callback(mock_update, mock_context)
        # Should not crash, callback just returns
        mock_update.callback_query.edit_message_text.assert_not_called()


# ── Past-time task callbacks ──────────────────────────────────────


class TestPastTimeCallbacks:
    @pytest.mark.asyncio
    async def test_past_task_tomorrow(self, mock_update, mock_context):
        from bot.models import ParsedTask
        parsed = ParsedTask(
            description="Past task", due_date="2026-03-10", due_time="08:00",
        )
        mock_context.user_data = {"pending_past_task": parsed}
        _set_callback(mock_update, "past_task_tomorrow")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Added" in text or "Past task" in text

    @pytest.mark.asyncio
    async def test_past_task_cancel(self, mock_update, mock_context):
        from bot.models import ParsedTask
        parsed = ParsedTask(description="Task", due_date="2026-03-10", due_time="08:00")
        mock_context.user_data = {"pending_past_task": parsed}
        _set_callback(mock_update, "past_task_cancel")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "Cancelled" in text

    @pytest.mark.asyncio
    async def test_past_task_no_pending(self, mock_update, mock_context):
        mock_context.user_data = {}
        _set_callback(mock_update, "past_task_tomorrow")
        await handle_callback(mock_update, mock_context)
        text = mock_update.callback_query.edit_message_text.call_args.args[0]
        assert "No pending" in text


# ── Authorization ─────────────────────────────────────────────────


class TestAuthorization:
    @pytest.mark.asyncio
    async def test_unauthorized_user_ignored(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_update.effective_user.id = 99999  # wrong user
        mock_update.callback_query.data = f"review_done_{tid}"
        await handle_callback(mock_update, mock_context)
        # Task should still be pending
        assert db.get_task(tid)["status"] == "pending"
