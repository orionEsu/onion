"""Tests for command handlers."""

import os
import time
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from bot import database as db
from bot.config import TIMEZONE
from bot.handlers import (
    _parse_user_time,
    add_command,
    tasks_command,
    upcoming_command,
    done_command,
    delete_command,
    edit_command,
    undo_command,
    status_command,
    history_command,
    labels_command,
    newlabel_command,
    editlabel_command,
    deletelabel_command,
    filter_command,
    stop_recurring_command,
    start_command,
    help_command,
    clear_command,
)


# ── _parse_user_time ──────────────────────────────────────────────


class TestParseUserTime:
    def test_12h_pm(self):
        assert _parse_user_time("3pm") == "15:00"

    def test_12h_am(self):
        assert _parse_user_time("9am") == "09:00"

    def test_12h_with_minutes(self):
        assert _parse_user_time("3:30pm") == "15:30"

    def test_24h(self):
        assert _parse_user_time("23:00") == "23:00"

    def test_12pm_is_noon(self):
        assert _parse_user_time("12pm") == "12:00"

    def test_12am_is_midnight(self):
        assert _parse_user_time("12am") == "00:00"

    def test_with_space(self):
        assert _parse_user_time("3 pm") == "15:00"

    def test_invalid(self):
        assert _parse_user_time("abc") is None

    def test_hour_out_of_range(self):
        assert _parse_user_time("25:00") is None


# ── Start & Help ──────────────────────────────────────────────────


class TestSimpleCommands:
    @pytest.mark.asyncio
    async def test_start_command(self, mock_update, mock_context):
        await start_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Task & Reminder Bot" in text

    @pytest.mark.asyncio
    async def test_help_command(self, mock_update, mock_context):
        await help_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "/add" in text


# ── /add command ──────────────────────────────────────────────────


class TestAddCommand:
    @pytest.mark.asyncio
    async def test_add_valid(self, mock_update, mock_context):
        mock_update.message.text = "/add Buy milk | 2026-03-10"
        await add_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args_list[0].args[0]
        assert "Added" in text

    @pytest.mark.asyncio
    async def test_add_with_time(self, mock_update, mock_context):
        mock_update.message.text = "/add Meeting | 2026-03-10 14:00"
        await add_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args_list[0].args[0]
        assert "Added" in text
        assert "14:00" in text

    @pytest.mark.asyncio
    async def test_add_no_pipe(self, mock_update, mock_context):
        mock_update.message.text = "/add Buy milk"
        await add_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Format" in text

    @pytest.mark.asyncio
    async def test_add_invalid_date(self, mock_update, mock_context):
        mock_update.message.text = "/add Task | not-a-date"
        await add_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Invalid" in text


# ── /tasks command ────────────────────────────────────────────────


class TestTasksCommand:
    @pytest.mark.asyncio
    async def test_tasks_empty(self, mock_update, mock_context):
        await tasks_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Nothing here" in text or "Today" in text

    @pytest.mark.asyncio
    async def test_tasks_with_data(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        db.add_task("Test task", today, None)
        await tasks_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Test task" in text


# ── /upcoming command ─────────────────────────────────────────────


class TestUpcomingCommand:
    @pytest.mark.asyncio
    async def test_upcoming_empty(self, mock_update, mock_context):
        await upcoming_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Upcoming" in text

    @pytest.mark.asyncio
    async def test_upcoming_with_data(self, mock_update, mock_context):
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        db.add_task("Future task", tomorrow, None)
        await upcoming_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Future task" in text


# ── /done command ─────────────────────────────────────────────────


class TestDoneCommand:
    @pytest.mark.asyncio
    async def test_done_no_args(self, mock_update, mock_context):
        mock_context.args = []
        await done_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Usage" in text

    @pytest.mark.asyncio
    async def test_done_invalid_id(self, mock_update, mock_context):
        mock_context.args = ["abc"]
        await done_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "number" in text

    @pytest.mark.asyncio
    async def test_done_not_found(self, mock_update, mock_context):
        mock_context.args = ["99999"]
        await done_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "not found" in text

    @pytest.mark.asyncio
    async def test_done_success(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_context.args = [str(tid)]
        await done_command(mock_update, mock_context)
        assert db.get_task(tid)["status"] == "done"
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Done" in text


# ── /delete command ───────────────────────────────────────────────


class TestDeleteCommand:
    @pytest.mark.asyncio
    async def test_delete_no_args(self, mock_update, mock_context):
        mock_context.args = []
        await delete_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Usage" in text

    @pytest.mark.asyncio
    async def test_delete_success(self, mock_update, mock_context):
        tid = db.add_task("To delete", "2026-03-10", None)
        mock_context.args = [str(tid)]
        await delete_command(mock_update, mock_context)
        assert db.get_task(tid) is None
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Deleted" in text


# ── /edit command ─────────────────────────────────────────────────


class TestEditCommand:
    @pytest.mark.asyncio
    async def test_edit_no_args(self, mock_update, mock_context):
        mock_context.args = []
        await edit_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Usage" in text

    @pytest.mark.asyncio
    async def test_edit_description(self, mock_update, mock_context):
        tid = db.add_task("Old name", "2026-03-10", None)
        mock_context.args = [str(tid), "desc", "New name"]
        await edit_command(mock_update, mock_context)
        assert db.get_task(tid)["description"] == "New name"

    @pytest.mark.asyncio
    async def test_edit_date(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_context.args = [str(tid), "date", "2026-04-01"]
        await edit_command(mock_update, mock_context)
        assert db.get_task(tid)["due_date"] == "2026-04-01"

    @pytest.mark.asyncio
    async def test_edit_time(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_context.args = [str(tid), "time", "15:00"]
        await edit_command(mock_update, mock_context)
        assert db.get_task(tid)["due_time"] == "15:00"

    @pytest.mark.asyncio
    async def test_edit_invalid_field(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_context.args = [str(tid), "color", "red"]
        await edit_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Field must be" in text

    @pytest.mark.asyncio
    async def test_edit_invalid_date(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        mock_context.args = [str(tid), "date", "not-a-date"]
        await edit_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Invalid date" in text

    @pytest.mark.asyncio
    async def test_edit_not_found(self, mock_update, mock_context):
        mock_context.args = ["99999", "desc", "New"]
        await edit_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "not found" in text


# ── /undo command ─────────────────────────────────────────────────


class TestUndoCommand:
    @pytest.mark.asyncio
    async def test_undo_nothing(self, mock_update, mock_context):
        await undo_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Nothing to undo" in text

    @pytest.mark.asyncio
    async def test_undo_done(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        task = db.get_task(tid)
        task_dict = {key: task[key] for key in task.keys()}
        db.update_task_status(tid, "done")

        mock_context.application.bot_data["last_undo"] = {
            "type": "done",
            "task_id": tid,
            "previous_state": task_dict,
            "timestamp": time.time(),
        }
        await undo_command(mock_update, mock_context)
        assert db.get_task(tid)["status"] == "pending"

    @pytest.mark.asyncio
    async def test_undo_delete(self, mock_update, mock_context):
        tid = db.add_task("Task", "2026-03-10", None)
        task = db.get_task(tid)
        task_dict = {key: task[key] for key in task.keys()}
        db.delete_task(tid)

        mock_context.application.bot_data["last_undo"] = {
            "type": "delete",
            "task_id": tid,
            "previous_state": task_dict,
            "timestamp": time.time(),
        }
        await undo_command(mock_update, mock_context)
        assert db.get_task(tid) is not None

    @pytest.mark.asyncio
    async def test_undo_expired(self, mock_update, mock_context):
        mock_context.application.bot_data["last_undo"] = {
            "type": "done",
            "task_id": 1,
            "previous_state": {},
            "timestamp": time.time() - 600,  # 10 minutes ago
        }
        await undo_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "5 minutes" in text


# ── /status command ───────────────────────────────────────────────


class TestStatusCommand:
    @pytest.mark.asyncio
    async def test_status_output(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        db.add_task("A", today, None)
        db.add_task("B", today, None)
        await status_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Status" in text
        assert "2" in text


# ── /history command ──────────────────────────────────────────────


class TestHistoryCommand:
    @pytest.mark.asyncio
    async def test_history_default_week(self, mock_update, mock_context):
        mock_context.args = []
        await history_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Completed" in text

    @pytest.mark.asyncio
    async def test_history_today(self, mock_update, mock_context):
        mock_context.args = ["today"]
        await history_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Today" in text


# ── Label commands ────────────────────────────────────────────────


class TestLabelCommands:
    @pytest.mark.asyncio
    async def test_labels_list(self, mock_update, mock_context):
        await labels_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Home" in text
        assert "Work" in text

    @pytest.mark.asyncio
    async def test_newlabel_success(self, mock_update, mock_context):
        mock_update.message.text = "/newlabel 🎵 Music"
        await newlabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Music" in text
        assert db.get_label_by_name("Music") is not None

    @pytest.mark.asyncio
    async def test_newlabel_no_args(self, mock_update, mock_context):
        mock_update.message.text = "/newlabel"
        await newlabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Usage" in text

    @pytest.mark.asyncio
    async def test_newlabel_duplicate(self, mock_update, mock_context):
        mock_update.message.text = "/newlabel 🏠 Home"
        await newlabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "already exists" in text

    @pytest.mark.asyncio
    async def test_editlabel_success(self, mock_update, mock_context):
        db.add_label("🎵", "OldMusic")
        mock_update.message.text = "/editlabel OldMusic 🎶 NewMusic"
        await editlabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "NewMusic" in text

    @pytest.mark.asyncio
    async def test_editlabel_not_found(self, mock_update, mock_context):
        mock_update.message.text = "/editlabel Nonexistent 🎶 New"
        await editlabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "not found" in text

    @pytest.mark.asyncio
    async def test_deletelabel_success(self, mock_update, mock_context):
        db.add_label("🧪", "TestDel")
        mock_update.message.text = "/deletelabel TestDel"
        await deletelabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "deleted" in text
        assert db.get_label_by_name("TestDel") is None

    @pytest.mark.asyncio
    async def test_deletelabel_not_found(self, mock_update, mock_context):
        mock_update.message.text = "/deletelabel Nonexistent"
        await deletelabel_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "not found" in text

    @pytest.mark.asyncio
    async def test_filter_command(self, mock_update, mock_context):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tid = db.add_task("Work stuff", today, None)
        work = db.get_label_by_name("Work")
        db.add_task_label(tid, work["id"])
        mock_update.message.text = "/filter Work"
        await filter_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Work stuff" in text

    @pytest.mark.asyncio
    async def test_filter_not_found(self, mock_update, mock_context):
        mock_update.message.text = "/filter Nonexistent"
        await filter_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "not found" in text


# ── /stoprecur command ────────────────────────────────────────────


class TestStopRecurCommand:
    @pytest.mark.asyncio
    async def test_stoprecur_no_args(self, mock_update, mock_context):
        mock_context.args = []
        await stop_recurring_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "Usage" in text

    @pytest.mark.asyncio
    async def test_stoprecur_not_recurring(self, mock_update, mock_context):
        tid = db.add_task("One-off", "2026-03-10", None)
        mock_context.args = [str(tid)]
        await stop_recurring_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "not a recurring" in text

    @pytest.mark.asyncio
    async def test_stoprecur_success(self, mock_update, mock_context):
        tid = db.add_task("Daily", "2026-03-10", None, recurrence_rule="daily")
        mock_context.args = [str(tid)]
        await stop_recurring_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        assert "stopped" in text
        assert db.get_task(tid)["recurrence_active"] == 0


# ── /clear command ────────────────────────────────────────────────


class TestClearCommand:
    @pytest.mark.asyncio
    async def test_clear_no_args(self, mock_update, mock_context):
        mock_update.message.text = "/clear"
        mock_context.args = []
        await clear_command(mock_update, mock_context)
        text = mock_update.message.reply_text.call_args.args[0]
        # Should ask what to clear
        assert "clear" in text.lower() or "Usage" in text
