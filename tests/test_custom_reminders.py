"""Tests for per-task custom reminders: absolute, offset, and repeating types."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from bot import database as db
from bot.config import TIMEZONE


# ── Database CRUD ────────────────────────────────────────────────


class TestCustomReminderCRUD:
    def test_add_absolute_reminder(self):
        tid = db.add_task("Meeting", "2026-03-20", "14:00")
        rid = db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")
        assert rid is not None
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 1
        assert reminders[0]["type"] == "absolute"
        assert reminders[0]["fire_at"] == "2026-03-20 13:00"
        assert reminders[0]["fired"] == 0

    def test_add_offset_reminder(self):
        tid = db.add_task("Deadline", "2026-03-20", "17:00")
        rid = db.add_custom_reminder(tid, "offset", offset_minutes=120)
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 1
        assert reminders[0]["type"] == "offset"
        assert reminders[0]["offset_minutes"] == 120
        assert reminders[0]["fired"] == 0

    def test_add_repeating_reminder(self):
        tid = db.add_task("Exam", "2026-03-20", "09:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 1
        assert reminders[0]["type"] == "repeating"
        assert reminders[0]["interval_minutes"] == 30
        assert reminders[0]["last_fired_at"] is None
        assert reminders[0]["fired"] == 0

    def test_multiple_reminders_per_task(self):
        tid = db.add_task("Big event", "2026-03-20", "10:00")
        db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 08:00")
        db.add_custom_reminder(tid, "offset", offset_minutes=60)
        db.add_custom_reminder(tid, "repeating", interval_minutes=15)
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 3
        types = {r["type"] for r in reminders}
        assert types == {"absolute", "offset", "repeating"}

    def test_get_custom_reminders_empty(self):
        tid = db.add_task("No reminders", "2026-03-20", None)
        assert db.get_custom_reminders_for_task(tid) == []


# ── Pending reminder queries ─────────────────────────────────────


class TestPendingAbsoluteReminders:
    def test_fires_when_time_reached(self):
        tid = db.add_task("Meeting", "2026-03-20", "14:00")
        db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        results = db.get_pending_absolute_reminders(now)
        assert len(results) == 1
        assert results[0]["task_id"] == tid

    def test_does_not_fire_before_time(self):
        tid = db.add_task("Meeting", "2026-03-20", "14:00")
        db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")
        now = datetime(2026, 3, 20, 12, 30, tzinfo=TIMEZONE)
        results = db.get_pending_absolute_reminders(now)
        assert len(results) == 0

    def test_does_not_fire_after_marked_fired(self):
        tid = db.add_task("Meeting", "2026-03-20", "14:00")
        rid = db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")
        db.mark_custom_reminder_fired(rid)
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        results = db.get_pending_absolute_reminders(now)
        assert len(results) == 0

    def test_skips_done_tasks(self):
        tid = db.add_task("Meeting", "2026-03-20", "14:00")
        db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")
        db.update_task_status(tid, "done")
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        results = db.get_pending_absolute_reminders(now)
        assert len(results) == 0


class TestPendingOffsetReminders:
    def test_fires_at_offset_before_due(self):
        tid = db.add_task("Call", "2026-03-20", "15:00")
        db.add_custom_reminder(tid, "offset", offset_minutes=120)
        # 2 hours before 15:00 = 13:00, check at 13:05
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        results = db.get_pending_offset_reminders(now)
        assert len(results) == 1
        assert results[0]["task_id"] == tid

    def test_does_not_fire_too_early(self):
        tid = db.add_task("Call", "2026-03-20", "15:00")
        db.add_custom_reminder(tid, "offset", offset_minutes=120)
        # 3 hours before = 12:00, should not fire
        now = datetime(2026, 3, 20, 12, 0, tzinfo=TIMEZONE)
        results = db.get_pending_offset_reminders(now)
        assert len(results) == 0

    def test_does_not_fire_after_marked_fired(self):
        tid = db.add_task("Call", "2026-03-20", "15:00")
        rid = db.add_custom_reminder(tid, "offset", offset_minutes=120)
        db.mark_custom_reminder_fired(rid)
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        results = db.get_pending_offset_reminders(now)
        assert len(results) == 0

    def test_skips_tasks_without_due_time(self):
        tid = db.add_task("Vague task", "2026-03-20", None)
        db.add_custom_reminder(tid, "offset", offset_minutes=60)
        now = datetime(2026, 3, 20, 14, 0, tzinfo=TIMEZONE)
        results = db.get_pending_offset_reminders(now)
        assert len(results) == 0


class TestPendingRepeatingReminders:
    def test_fires_first_time(self):
        tid = db.add_task("Exam prep", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        # Should fire immediately since last_fired_at is NULL and due time hasn't passed
        now = datetime(2026, 3, 20, 16, 0, tzinfo=TIMEZONE)
        results = db.get_pending_repeating_reminders(now)
        assert len(results) == 1

    def test_fires_after_interval_elapsed(self):
        tid = db.add_task("Exam prep", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        # Simulate first fire
        db.mark_custom_reminder_fired(rid, fired_at="2026-03-20 16:00")
        # 35 minutes later — should fire again
        now = datetime(2026, 3, 20, 16, 35, tzinfo=TIMEZONE)
        results = db.get_pending_repeating_reminders(now)
        assert len(results) == 1

    def test_does_not_fire_before_interval(self):
        tid = db.add_task("Exam prep", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        db.mark_custom_reminder_fired(rid, fired_at="2026-03-20 16:00")
        # Only 10 minutes later — too soon
        now = datetime(2026, 3, 20, 16, 10, tzinfo=TIMEZONE)
        results = db.get_pending_repeating_reminders(now)
        assert len(results) == 0

    def test_does_not_fire_after_due_time(self):
        tid = db.add_task("Exam prep", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        # After due time — should not fire
        now = datetime(2026, 3, 20, 18, 5, tzinfo=TIMEZONE)
        results = db.get_pending_repeating_reminders(now)
        assert len(results) == 0

    def test_does_not_fire_when_marked_done(self):
        tid = db.add_task("Exam prep", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        db.mark_repeating_reminder_done(rid)
        now = datetime(2026, 3, 20, 16, 0, tzinfo=TIMEZONE)
        results = db.get_pending_repeating_reminders(now)
        assert len(results) == 0


# ── Cleanup / cascade ────────────────────────────────────────────


class TestCustomReminderCleanup:
    def test_clear_reminders_for_task_clears_custom(self):
        tid = db.add_task("Task", "2026-03-20", "10:00")
        db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 09:00")
        db.add_custom_reminder(tid, "repeating", interval_minutes=15)
        assert len(db.get_custom_reminders_for_task(tid)) == 2
        db.clear_reminders_for_task(tid)
        assert len(db.get_custom_reminders_for_task(tid)) == 0

    def test_delete_task_cascades_custom_reminders(self):
        tid = db.add_task("Task", "2026-03-20", "10:00")
        db.add_custom_reminder(tid, "offset", offset_minutes=60)
        db.delete_task(tid)
        assert db.get_custom_reminders_for_task(tid) == []

    def test_reschedule_clears_custom_reminders(self):
        tid = db.add_task("Task", "2026-03-20", "14:00")
        db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")
        db.carry_over_task(tid, "2026-03-21", "14:00")
        assert db.get_custom_reminders_for_task(tid) == []


# ── Handler logic ────────────────────────────────────────────────


class TestSetReminderHandler:
    @pytest.mark.asyncio
    async def test_set_absolute_reminder(self, mock_update, mock_context):
        tid = db.add_task("Meeting", "2026-04-01", "14:00")
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "absolute",
            "time": "13:00",
            "date": "2026-04-01",
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        mock_update.message.reply_text.assert_called_once()
        msg = mock_update.message.reply_text.call_args[0][0]
        assert "13:00" in msg
        # Verify DB
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 1
        assert reminders[0]["type"] == "absolute"

    @pytest.mark.asyncio
    async def test_set_offset_reminder(self, mock_update, mock_context):
        tid = db.add_task("Deadline", "2026-04-01", "17:00")
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "offset",
            "offset_minutes": 120,
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        mock_update.message.reply_text.assert_called_once()
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 1
        assert reminders[0]["offset_minutes"] == 120

    @pytest.mark.asyncio
    async def test_set_repeating_reminder(self, mock_update, mock_context):
        tid = db.add_task("Exam", "2026-04-01", "09:00")
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "repeating",
            "interval_minutes": 30,
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        mock_update.message.reply_text.assert_called_once()
        reminders = db.get_custom_reminders_for_task(tid)
        assert len(reminders) == 1
        assert reminders[0]["interval_minutes"] == 30

    @pytest.mark.asyncio
    async def test_offset_reminder_requires_due_time(self, mock_update, mock_context):
        tid = db.add_task("Vague", "2026-04-01", None)  # no time
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "offset",
            "offset_minutes": 60,
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        msg = mock_update.message.reply_text.call_args[0][0]
        assert "no due time" in msg.lower() or "no time" in msg.lower()
        # No reminder should be created
        assert db.get_custom_reminders_for_task(tid) == []

    @pytest.mark.asyncio
    async def test_repeating_reminder_requires_due_time(self, mock_update, mock_context):
        tid = db.add_task("Vague", "2026-04-01", None)
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "repeating",
            "interval_minutes": 15,
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        msg = mock_update.message.reply_text.call_args[0][0]
        assert "no due time" in msg.lower() or "no time" in msg.lower()
        assert db.get_custom_reminders_for_task(tid) == []

    @pytest.mark.asyncio
    async def test_absolute_reminder_past_time_rejected(self, mock_update, mock_context):
        tid = db.add_task("Meeting", "2020-01-01", "14:00")  # past date
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "absolute",
            "time": "13:00",
            "date": "2020-01-01",
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        msg = mock_update.message.reply_text.call_args[0][0]
        assert "passed" in msg.lower()
        assert db.get_custom_reminders_for_task(tid) == []

    @pytest.mark.asyncio
    async def test_invalid_reminder_type_rejected(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-04-01", "10:00")
        mock_context.application.bot_data["task_pos_map"] = {1: tid}

        from bot.handlers import _route_intent
        data = {
            "intent": "set_reminder",
            "task_id": 1,
            "task_description": None,
            "reminder_type": "bogus",
        }
        await _route_intent(mock_update, mock_context, data, "set_reminder")
        msg = mock_update.message.reply_text.call_args[0][0]
        assert "couldn" in msg.lower() or "error" in msg.lower()


# ── Scheduler ────────────────────────────────────────────────────


class TestCustomReminderScheduler:
    @pytest.mark.asyncio
    async def test_absolute_reminder_fires(self, mock_context):
        tid = db.add_task("Meeting", "2026-03-20", "14:00")
        rid = db.add_custom_reminder(tid, "absolute", fire_at="2026-03-20 13:00")

        from bot.scheduler import _check_custom_reminders
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        await _check_custom_reminders(mock_context, now)

        mock_context.bot.send_message.assert_called_once()
        # Verify marked as fired
        reminders = db.get_custom_reminders_for_task(tid)
        assert reminders[0]["fired"] == 1

    @pytest.mark.asyncio
    async def test_offset_reminder_fires(self, mock_context):
        tid = db.add_task("Call", "2026-03-20", "15:00")
        rid = db.add_custom_reminder(tid, "offset", offset_minutes=120)

        from bot.scheduler import _check_custom_reminders
        now = datetime(2026, 3, 20, 13, 5, tzinfo=TIMEZONE)
        await _check_custom_reminders(mock_context, now)

        mock_context.bot.send_message.assert_called_once()
        reminders = db.get_custom_reminders_for_task(tid)
        assert reminders[0]["fired"] == 1

    @pytest.mark.asyncio
    async def test_repeating_reminder_fires_and_updates_last_fired(self, mock_context):
        tid = db.add_task("Study", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)

        from bot.scheduler import _check_custom_reminders
        now = datetime(2026, 3, 20, 16, 0, tzinfo=TIMEZONE)
        await _check_custom_reminders(mock_context, now)

        mock_context.bot.send_message.assert_called_once()
        reminders = db.get_custom_reminders_for_task(tid)
        assert reminders[0]["last_fired_at"] is not None
        assert reminders[0]["fired"] == 0  # still active, not done

    @pytest.mark.asyncio
    async def test_repeating_reminder_respects_interval(self, mock_context):
        tid = db.add_task("Study", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)
        db.mark_custom_reminder_fired(rid, fired_at="2026-03-20 16:00")

        from bot.scheduler import _check_custom_reminders

        # Too soon — 10 min after last fire
        now = datetime(2026, 3, 20, 16, 10, tzinfo=TIMEZONE)
        await _check_custom_reminders(mock_context, now)
        mock_context.bot.send_message.assert_not_called()

        # After interval — 35 min after last fire
        now = datetime(2026, 3, 20, 16, 35, tzinfo=TIMEZONE)
        await _check_custom_reminders(mock_context, now)
        mock_context.bot.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_repeating_reminder_stops_after_due(self, mock_context):
        tid = db.add_task("Study", "2026-03-20", "18:00")
        rid = db.add_custom_reminder(tid, "repeating", interval_minutes=30)

        from bot.scheduler import _check_custom_reminders
        # After due time
        now = datetime(2026, 3, 20, 18, 5, tzinfo=TIMEZONE)
        await _check_custom_reminders(mock_context, now)

        mock_context.bot.send_message.assert_not_called()
