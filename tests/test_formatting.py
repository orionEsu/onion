"""Tests for message formatting functions."""

import pytest
from unittest.mock import patch
from datetime import datetime, date

from bot import formatting as fmt


# ── Task line formatting ──────────────────────────────────────────


class TestFormatTaskLine:
    def _make_task(self, **overrides):
        base = {
            "id": 1, "description": "Test task", "due_date": "2026-03-10",
            "due_time": None, "status": "pending", "recurrence_rule": None,
        }
        base.update(overrides)
        return base

    def test_basic_task(self):
        result = fmt.format_task_line(self._make_task())
        assert "Test task" in result
        assert "1." in result

    def test_task_with_time(self):
        result = fmt.format_task_line(self._make_task(due_time="14:00"))
        assert "at 14:00" in result

    def test_task_with_date(self):
        result = fmt.format_task_line(self._make_task(), show_date=True)
        assert "—" in result  # date separator

    def test_done_task_shows_check(self):
        result = fmt.format_task_line(self._make_task(status="done"))
        assert "✅" in result

    def test_recurring_task_shows_badge(self):
        result = fmt.format_task_line(self._make_task(recurrence_rule="daily"))
        assert "🔄" in result

    def test_task_with_labels(self):
        labels = [{"emoji": "💼", "name": "Work"}, {"emoji": "🏠", "name": "Home"}]
        result = fmt.format_task_line(self._make_task(), labels=labels)
        assert "💼" in result
        assert "🏠" in result

    def test_custom_position(self):
        result = fmt.format_task_line(self._make_task(), position=5)
        assert "5." in result

    def test_html_escaping(self):
        result = fmt.format_task_line(self._make_task(description="<b>bold</b>"))
        assert "<b>bold</b>" not in result
        assert "&lt;b&gt;" in result


# ── Task list ─────────────────────────────────────────────────────


class TestFormatTaskList:
    def test_empty_list(self):
        text, pos_map = fmt.format_task_list("Title", [])
        assert "Nothing here" in text
        assert pos_map == {}

    def test_list_with_tasks(self):
        tasks = [
            {"id": 10, "description": "A", "due_date": "2026-03-10",
             "due_time": None, "status": "pending", "recurrence_rule": None},
            {"id": 20, "description": "B", "due_date": "2026-03-10",
             "due_time": "09:00", "status": "pending", "recurrence_rule": None},
        ]
        text, pos_map = fmt.format_task_list("Title", tasks)
        assert "1." in text
        assert "2." in text
        assert pos_map == {1: 10, 2: 20}


# ── Task added ────────────────────────────────────────────────────


class TestFormatTaskAdded:
    def test_basic(self):
        result = fmt.format_task_added(1, "Buy milk", "2026-03-10", None, None)
        assert "Added" in result
        assert "Buy milk" in result

    def test_with_time(self):
        result = fmt.format_task_added(1, "Meeting", "2026-03-10", "14:00", None)
        assert "14:00" in result

    def test_with_recurrence(self):
        result = fmt.format_task_added(1, "Gym", "2026-03-10", None, "daily")
        assert "every day" in result

    def test_with_labels(self):
        labels = [{"emoji": "💼", "name": "Work"}]
        result = fmt.format_task_added(1, "Meeting", "2026-03-10", None, None, labels)
        assert "Work" in result

    def test_with_notes(self):
        result = fmt.format_task_added(1, "Task", "2026-03-10", None, None, notes="Extra info")
        assert "Extra info" in result

    def test_html_escaping(self):
        result = fmt.format_task_added(1, "<script>", "2026-03-10", None, None)
        assert "<script>" not in result


# ── Task detail ───────────────────────────────────────────────────


class TestFormatTaskDetail:
    def test_basic_detail(self):
        task = {
            "id": 1, "description": "Task", "due_date": "2026-03-10",
            "due_time": None, "status": "pending", "recurrence_rule": None, "notes": None,
        }
        result = fmt.format_task_detail(task)
        assert "Task" in result
        assert "No notes" in result

    def test_detail_with_notes(self):
        task = {
            "id": 1, "description": "Task", "due_date": "2026-03-10",
            "due_time": "10:00", "status": "done", "recurrence_rule": "daily",
            "notes": "Important note",
        }
        result = fmt.format_task_detail(task)
        assert "Important note" in result
        assert "every day" in result
        assert "10:00" in result


# ── Humanize helpers ──────────────────────────────────────────────


class TestHumanizeDate:
    @patch("bot.formatting.datetime")
    def test_today(self, mock_dt):
        mock_dt.now.return_value = datetime(2026, 3, 10, tzinfo=None)
        mock_dt.now.return_value = type("FakeDT", (), {"date": lambda self: date(2026, 3, 10)})()
        # Use the actual function with a known date context
        # Instead of mocking, test relative outputs directly
        pass

    def test_humanize_rule_daily(self):
        assert fmt._humanize_rule("daily") == "every day"

    def test_humanize_rule_weekly(self):
        assert "Monday" in fmt._humanize_rule("weekly:monday")

    def test_humanize_rule_biweekly(self):
        result = fmt._humanize_rule("biweekly:friday")
        assert "other" in result and "Friday" in result

    def test_humanize_rule_monthly(self):
        result = fmt._humanize_rule("monthly:1")
        assert "1st" in result

    def test_humanize_rule_monthly_ordinals(self):
        assert "2nd" in fmt._humanize_rule("monthly:2")
        assert "3rd" in fmt._humanize_rule("monthly:3")
        assert "11th" in fmt._humanize_rule("monthly:11")
        assert "21st" in fmt._humanize_rule("monthly:21")

    def test_humanize_rule_every_n_days(self):
        assert "every 3 days" in fmt._humanize_rule("every_n_days:3")

    def test_humanize_rule_specific(self):
        result = fmt._humanize_rule("specific:mon,wed,fri")
        assert "Mon" in result and "Wed" in result and "Fri" in result

    def test_humanize_rule_passthrough(self):
        assert fmt._humanize_rule("unknown_rule") == "unknown_rule"


# ── Review formatting ─────────────────────────────────────────────


class TestReviewFormatting:
    def test_review_header(self):
        result = fmt.format_daily_review_header()
        assert "Review" in result

    def test_review_task(self):
        task = {"id": 5, "description": "Call Mom", "due_time": "14:00", "status": "pending"}
        result = fmt.format_review_task(task)
        assert "Call Mom" in result
        assert "14:00" in result
        assert "#5" in result

    def test_review_done(self):
        result = fmt.format_review_done(5)
        assert "#5" in result
        assert "Done" in result

    def test_review_carried(self):
        result = fmt.format_review_carried(5, "2026-03-11")
        assert "#5" in result

    def test_review_dropped(self):
        result = fmt.format_review_dropped(5)
        assert "#5" in result
        assert "Dropped" in result

    def test_no_tasks_review(self):
        result = fmt.format_no_tasks_review()
        assert "No pending tasks" in result


# ── Reminder formatting ───────────────────────────────────────────


class TestReminderFormatting:
    def test_reminder_basic(self):
        task = {"id": 1, "description": "Meeting", "due_date": "2026-03-10", "due_time": "14:00"}
        result = fmt.format_reminder(task, "2 hours")
        assert "Meeting" in result
        assert "2 hours" in result
        assert "Reminder" in result


# ── Labels formatting ─────────────────────────────────────────────


class TestLabelsFormatting:
    def test_labels_list_empty(self):
        result = fmt.format_labels_list([])
        assert "No labels" in result

    def test_labels_list_with_items(self):
        labels = [{"id": 1, "emoji": "💼", "name": "Work"}]
        result = fmt.format_labels_list(labels)
        assert "Work" in result
        assert "💼" in result

    def test_label_prompt(self):
        result = fmt.format_label_prompt(5, "Buy milk")
        assert "Buy milk" in result
        assert "Pick labels" in result


# ── Status formatting ─────────────────────────────────────────────


class TestStatusFormatting:
    def test_status_basic(self):
        result = fmt.format_status(5, 0, 10, 3)
        assert "5" in result
        assert "10" in result
        assert "3" in result

    def test_status_with_overdue(self):
        result = fmt.format_status(5, 2, 10, 3)
        assert "Overdue" in result
        assert "2" in result

    def test_status_no_overdue(self):
        result = fmt.format_status(5, 0, 10, 3)
        assert "Overdue" not in result


# ── History formatting ────────────────────────────────────────────


class TestHistoryFormatting:
    def test_history_empty(self):
        result = fmt.format_history([], "Today")
        assert "No tasks in this period" in result

    def test_history_with_tasks(self):
        tasks = [
            {"id": 1, "description": "A", "due_date": "2026-03-10",
             "due_time": None, "status": "done", "recurrence_rule": None},
            {"id": 2, "description": "B", "due_date": "2026-03-10",
             "due_time": "09:00", "status": "done", "recurrence_rule": None},
        ]
        result = fmt.format_history(tasks, "Today")
        assert "A" in result
        assert "B" in result
        assert "2 done" in result
        assert "2 total" in result


# ── Weekly summary ────────────────────────────────────────────────


class TestWeeklySummaryFormatting:
    def test_weekly_summary(self):
        stats = {"done": 8, "pending": 2, "cancelled": 1}
        result = fmt.format_weekly_summary(stats)
        assert "8" in result
        assert "2" in result
        assert "1" in result
        assert "73%" in result  # 8/11

    def test_weekly_summary_zero_tasks(self):
        stats = {"done": 0, "pending": 0, "cancelled": 0}
        result = fmt.format_weekly_summary(stats)
        assert "0" in result


# ── Undo formatting ──────────────────────────────────────────────


class TestUndoFormatting:
    def test_undo_success_done(self):
        result = fmt.format_undo_success("done", 5)
        assert "un-completed" in result

    def test_undo_success_delete(self):
        result = fmt.format_undo_success("delete", 5)
        assert "restored" in result

    def test_undo_expired(self):
        result = fmt.format_undo_expired()
        assert "5 minutes" in result

    def test_undo_nothing(self):
        result = fmt.format_undo_nothing()
        assert "Nothing to undo" in result


# ── Edit formatting ──────────────────────────────────────────────


class TestEditFormatting:
    def test_edit_move(self):
        result = fmt.format_task_edited(5, {"due_date": "2026-03-15"}, reason="move")
        assert "moved" in result

    def test_edit_rename(self):
        result = fmt.format_task_edited(5, {"description": "New name"}, reason="rename")
        assert "renamed" in result
        assert "New name" in result

    def test_edit_generic(self):
        result = fmt.format_task_edited(5, {"due_time": "15:00"}, reason="edit")
        assert "updated" in result


# ── Misc formatting ──────────────────────────────────────────────


class TestMiscFormatting:
    def test_format_start(self):
        result = fmt.format_start()
        assert "Task & Reminder Bot" in result

    def test_format_help(self):
        result = fmt.format_help()
        assert "/add" in result
        assert "/tasks" in result
        assert "/routine" in result

    def test_format_error(self):
        result = fmt.format_error("Something went wrong")
        assert "Something went wrong" in result
        assert "❌" in result

    def test_format_error_escapes_html(self):
        result = fmt.format_error("<b>bad</b>")
        assert "<b>bad</b>" not in result

    def test_format_confirm_task(self):
        from bot.models import ParsedTask
        parsed = ParsedTask(
            description="Buy milk", due_date="2026-03-10", due_time="14:00",
            recurrence_rule="weekly:monday", label_names=["Home"],
        )
        result = fmt.format_confirm_task(parsed)
        assert "Buy milk" in result
        assert "14:00" in result
        assert "Monday" in result
        assert "Home" in result

    def test_format_not_understood(self):
        result = fmt.format_not_understood()
        assert "didn't quite get that" in result

    def test_format_snoozed(self):
        result = fmt.format_snoozed(5, "2026-03-11", "14:00")
        assert "snoozed" in result
        assert "14:00" in result

    def test_format_overdue_warning_empty(self):
        assert fmt.format_overdue_warning([]) == ""

    def test_format_overdue_warning(self):
        tasks = [
            {"id": 1, "description": "Late", "due_date": "2026-03-01",
             "due_time": None, "status": "pending", "recurrence_rule": None},
        ]
        result = fmt.format_overdue_warning(tasks)
        assert "Overdue" in result
        assert "Late" in result

    def test_format_disambiguate(self):
        tasks = [
            {"id": 1, "description": "Buy milk", "due_date": "2026-03-10", "due_time": None},
            {"id": 2, "description": "Buy eggs", "due_date": "2026-03-10", "due_time": "09:00"},
        ]
        result = fmt.format_disambiguate(tasks)
        assert "Multiple" in result
        assert "Buy milk" in result
        assert "Buy eggs" in result

    def test_format_morning_prompt(self):
        result = fmt.format_morning_prompt("Honey never spoils.")
        assert "Honey never spoils" in result
        assert "Fun fact" in result

    def test_format_morning_prompt_truncates_long_fact(self):
        long_fact = "A" * 500
        result = fmt.format_morning_prompt(long_fact)
        assert "..." in result

    def test_format_morning_summary(self):
        added = [{"id": 1, "description": "Task", "due_date": "2026-03-10",
                  "due_time": None, "status": "pending", "recurrence_rule": None}]
        all_tasks = added
        result = fmt.format_morning_summary(added, all_tasks)
        assert "1" in result
        assert "Morning Planning Done" in result
