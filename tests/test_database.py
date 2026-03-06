"""Tests for database operations: task CRUD, recurrence, labels, and clear."""

import pytest
from datetime import datetime, timedelta

from bot import database as db
from bot.config import TIMEZONE


# ── Task CRUD ─────────────────────────────────────────────────────


class TestTaskCRUD:
    def test_add_task_basic(self):
        tid = db.add_task("Buy groceries", "2026-03-10", None)
        task = db.get_task(tid)
        assert task is not None
        assert task["description"] == "Buy groceries"
        assert task["due_date"] == "2026-03-10"
        assert task["due_time"] is None
        assert task["status"] == "pending"

    def test_add_task_with_time(self):
        tid = db.add_task("Meeting", "2026-03-10", "14:00")
        task = db.get_task(tid)
        assert task["due_time"] == "14:00"

    def test_add_task_with_notes(self):
        tid = db.add_task("Shopping", "2026-03-10", None, notes="Need milk and eggs")
        task = db.get_task(tid)
        assert task["notes"] == "Need milk and eggs"

    def test_add_task_with_recurrence(self):
        tid = db.add_task("Gym", "2026-03-10", "07:00", recurrence_rule="daily")
        task = db.get_task(tid)
        assert task["recurrence_rule"] == "daily"
        assert task["recurrence_active"] == 1

    def test_add_task_invalid_recurrence_ignored(self):
        tid = db.add_task("Task", "2026-03-10", None, recurrence_rule="invalid_rule")
        task = db.get_task(tid)
        assert task["recurrence_rule"] is None
        assert task["recurrence_active"] == 0

    def test_get_task_not_found(self):
        assert db.get_task(99999) is None

    def test_get_tasks_for_date(self):
        db.add_task("Task A", "2026-03-10", "09:00")
        db.add_task("Task B", "2026-03-10", None)
        db.add_task("Task C", "2026-03-11", None)
        tasks = db.get_tasks_for_date("2026-03-10")
        assert len(tasks) == 2
        # Tasks with time should come first
        assert tasks[0]["due_time"] == "09:00"

    def test_get_tasks_for_date_excludes_done(self):
        tid = db.add_task("Done task", "2026-03-10", None)
        db.update_task_status(tid, "done")
        db.add_task("Pending task", "2026-03-10", None)
        tasks = db.get_tasks_for_date("2026-03-10")
        assert len(tasks) == 1
        assert tasks[0]["description"] == "Pending task"

    def test_get_tasks_for_date_empty(self):
        tasks = db.get_tasks_for_date("2099-01-01")
        assert tasks == []

    def test_update_task_status(self):
        tid = db.add_task("Task", "2026-03-10", None)
        db.update_task_status(tid, "done")
        assert db.get_task(tid)["status"] == "done"

    def test_update_task_status_invalid(self):
        tid = db.add_task("Task", "2026-03-10", None)
        with pytest.raises(ValueError):
            db.update_task_status(tid, "bogus")

    def test_delete_task(self):
        tid = db.add_task("To delete", "2026-03-10", None)
        db.delete_task(tid)
        assert db.get_task(tid) is None

    def test_update_task_description(self):
        tid = db.add_task("Old desc", "2026-03-10", None)
        db.update_task(tid, description="New desc")
        assert db.get_task(tid)["description"] == "New desc"

    def test_update_task_date_resets_reminders(self):
        tid = db.add_task("Task", "2026-03-10", "10:00")
        db.mark_reminder_sent(tid, "24h")
        assert db.get_task(tid)["reminder_24h"] == 1
        db.update_task(tid, due_date="2026-03-15")
        task = db.get_task(tid)
        assert task["due_date"] == "2026-03-15"
        assert task["reminder_24h"] == 0

    def test_update_task_returns_false_no_changes(self):
        tid = db.add_task("Task", "2026-03-10", None)
        assert db.update_task(tid) is False

    def test_update_task_notes(self):
        tid = db.add_task("Task", "2026-03-10", None)
        db.update_task_notes(tid, "Some notes")
        assert db.get_task(tid)["notes"] == "Some notes"
        db.update_task_notes(tid, None)
        assert db.get_task(tid)["notes"] is None

    def test_carry_over_task(self):
        tid = db.add_task("Task", "2026-03-10", "10:00")
        db.mark_reminder_sent(tid, "24h")
        db.carry_over_task(tid, "2026-03-12", "15:00")
        task = db.get_task(tid)
        assert task["due_date"] == "2026-03-12"
        assert task["due_time"] == "15:00"
        assert task["reminder_24h"] == 0

    def test_get_upcoming_tasks(self):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        db.add_task("Today task", today, None)
        db.add_task("Tomorrow task", tomorrow, None)
        db.add_task("Past task", "2020-01-01", None)  # past
        upcoming = db.get_upcoming_tasks()
        descs = [t["description"] for t in upcoming]
        assert "Today task" in descs
        assert "Tomorrow task" in descs
        assert "Past task" not in descs

    def test_get_overdue_tasks(self):
        db.add_task("Overdue", "2020-01-01", None)
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        db.add_task("Not overdue", today, None)
        overdue = db.get_overdue_tasks()
        descs = [t["description"] for t in overdue]
        assert "Overdue" in descs
        assert "Not overdue" not in descs

    def test_find_tasks_by_description(self):
        db.add_task("Buy milk", "2026-03-10", None)
        db.add_task("Buy eggs", "2026-03-10", None)
        db.add_task("Go to gym", "2026-03-10", None)
        results = db.find_tasks_by_description("buy")
        assert len(results) == 2

    def test_find_tasks_by_description_case_insensitive(self):
        db.add_task("Call Mom", "2026-03-10", None)
        results = db.find_tasks_by_description("CALL")
        assert len(results) == 1

    def test_reinsert_task(self):
        tid = db.add_task("Original", "2026-03-10", "09:00")
        task = db.get_task(tid)
        task_dict = {key: task[key] for key in task.keys()}
        db.delete_task(tid)
        assert db.get_task(tid) is None
        db.reinsert_task(task_dict)
        restored = db.get_task(tid)
        assert restored is not None
        assert restored["description"] == "Original"

    def test_get_completed_tasks_for_date(self):
        tid = db.add_task("Done today", "2026-03-10", None)
        db.update_task_status(tid, "done")
        db.add_task("Still pending", "2026-03-10", None)
        completed = db.get_completed_tasks_for_date("2026-03-10")
        assert len(completed) == 1
        assert completed[0]["description"] == "Done today"

    def test_get_completed_tasks_with_range(self):
        t1 = db.add_task("A", "2026-03-08", None)
        t2 = db.add_task("B", "2026-03-10", None)
        t3 = db.add_task("C", "2026-03-15", None)
        for t in (t1, t2, t3):
            db.update_task_status(t, "done")
        results = db.get_completed_tasks("2026-03-09", "2026-03-12")
        assert len(results) == 1
        assert results[0]["description"] == "B"

    def test_get_completed_tasks_all(self):
        t1 = db.add_task("A", "2020-01-01", None)
        t2 = db.add_task("B", "2026-12-31", None)
        db.update_task_status(t1, "done")
        db.update_task_status(t2, "done")
        results = db.get_completed_tasks()
        assert len(results) >= 2

    def test_get_weekly_stats(self):
        t1 = db.add_task("Done", "2026-03-10", None)
        db.update_task_status(t1, "done")
        db.add_task("Pending", "2026-03-10", None)
        t3 = db.add_task("Cancelled", "2026-03-10", None)
        db.update_task_status(t3, "cancelled")
        stats = db.get_weekly_stats("2026-03-10", "2026-03-10")
        assert stats["done"] == 1
        assert stats["pending"] == 1
        assert stats["cancelled"] == 1


# ── Clear ─────────────────────────────────────────────────────────


class TestClearTasks:
    def test_clear_all_tasks(self):
        db.add_task("A", "2026-03-10", None)
        db.add_task("B", "2026-03-11", None)
        count = db.clear_tasks("all_tasks")
        assert count == 2
        assert db.get_upcoming_tasks() == []

    def test_clear_all_labels(self):
        # There are 5 preset labels
        count = db.clear_tasks("all_labels")
        assert count >= 5
        assert db.get_all_labels() == []

    def test_clear_everything(self):
        db.add_task("Task", "2026-03-10", None)
        count = db.clear_tasks("everything")
        assert count >= 1

    def test_clear_invalid_scope(self):
        assert db.clear_tasks("bogus") == 0


# ── Recurrence ────────────────────────────────────────────────────


class TestRecurrence:
    def test_compute_next_date_daily(self):
        assert db.compute_next_date("2026-03-10", "daily") == "2026-03-11"

    def test_compute_next_date_every_n_days(self):
        assert db.compute_next_date("2026-03-10", "every_n_days:3") == "2026-03-13"

    def test_compute_next_date_every_n_days_invalid(self):
        assert db.compute_next_date("2026-03-10", "every_n_days:0") is None
        assert db.compute_next_date("2026-03-10", "every_n_days:abc") is None

    def test_compute_next_date_weekly(self):
        # 2026-03-10 is a Tuesday
        result = db.compute_next_date("2026-03-10", "weekly:tuesday")
        assert result == "2026-03-17"  # next Tuesday

    def test_compute_next_date_weekly_different_day(self):
        # 2026-03-10 is Tuesday, next Friday
        result = db.compute_next_date("2026-03-10", "weekly:friday")
        assert result == "2026-03-13"

    def test_compute_next_date_weekly_invalid_day(self):
        assert db.compute_next_date("2026-03-10", "weekly:foobar") is None

    def test_compute_next_date_biweekly(self):
        result = db.compute_next_date("2026-03-10", "biweekly:tuesday")
        assert result == "2026-03-24"  # 2 weeks later

    def test_compute_next_date_monthly(self):
        result = db.compute_next_date("2026-03-10", "monthly:15")
        assert result == "2026-03-15"

    def test_compute_next_date_monthly_past_day(self):
        result = db.compute_next_date("2026-03-20", "monthly:15")
        assert result == "2026-04-15"

    def test_compute_next_date_monthly_overflow(self):
        # Feb doesn't have 31 days
        result = db.compute_next_date("2026-01-31", "monthly:31")
        assert result == "2026-02-28"

    def test_compute_next_date_specific_days(self):
        # 2026-03-10 is Tuesday, next Mon/Wed/Fri
        result = db.compute_next_date("2026-03-10", "specific:mon,wed,fri")
        assert result == "2026-03-11"  # Wednesday

    def test_compute_next_date_invalid_rule(self):
        assert db.compute_next_date("2026-03-10", "nonsense") is None

    def test_compute_next_date_invalid_date(self):
        assert db.compute_next_date("not-a-date", "daily") is None

    def test_create_next_occurrence(self):
        tid = db.add_task("Daily task", "2026-03-10", "08:00", recurrence_rule="daily")
        next_id = db.create_next_occurrence(tid)
        assert next_id is not None
        next_task = db.get_task(next_id)
        assert next_task["due_date"] == "2026-03-11"
        assert next_task["due_time"] == "08:00"
        assert next_task["recurrence_rule"] == "daily"
        assert next_task["parent_task_id"] == tid

    def test_create_next_occurrence_no_recurrence(self):
        tid = db.add_task("One-off", "2026-03-10", None)
        assert db.create_next_occurrence(tid) is None

    def test_create_next_occurrence_copies_labels(self):
        tid = db.add_task("Task", "2026-03-10", None, recurrence_rule="daily")
        label = db.get_label_by_name("Health")
        db.add_task_label(tid, label["id"])
        next_id = db.create_next_occurrence(tid)
        next_labels = db.get_labels_for_task(next_id)
        assert any(l["name"] == "Health" for l in next_labels)

    def test_stop_recurrence(self):
        tid = db.add_task("Recurring", "2026-03-10", None, recurrence_rule="daily")
        next_id = db.create_next_occurrence(tid)
        db.stop_recurrence(tid)
        assert db.get_task(tid)["recurrence_active"] == 0
        assert db.get_task(tid)["recurrence_rule"] is None
        assert db.get_task(next_id)["recurrence_active"] == 0

    def test_validate_recurrence_rules(self):
        valid_rules = [
            "daily",
            "every_n_days:2",
            "weekly:monday",
            "biweekly:friday",
            "monthly:15",
            "specific:mon,wed,fri",
        ]
        for rule in valid_rules:
            tid = db.add_task("Task", "2026-03-10", None, recurrence_rule=rule)
            assert db.get_task(tid)["recurrence_rule"] == rule, f"Rule {rule} should be valid"

    def test_generate_recurring_for_today(self):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        yesterday = (datetime.now(TIMEZONE) - timedelta(days=1)).strftime("%Y-%m-%d")
        tid = db.add_task("Daily", yesterday, "08:00", recurrence_rule="daily")
        # Mark it done so there's no pending task
        db.update_task_status(tid, "done")
        created = db.generate_recurring_for_today()
        assert len(created) >= 1
        new_task = db.get_task(created[0])
        assert new_task["due_date"] == today
        assert new_task["description"] == "Daily"

    def test_generate_recurring_no_duplicates(self):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        db.add_task("Daily", today, None, recurrence_rule="daily")
        # Already has a pending task for today, should not duplicate
        created = db.generate_recurring_for_today()
        assert len(created) == 0


# ── Labels ────────────────────────────────────────────────────────


class TestLabels:
    def test_preset_labels_exist(self):
        labels = db.get_all_labels()
        names = [l["name"] for l in labels]
        for expected in ("Home", "Work", "Health", "Learning", "Errands"):
            assert expected in names

    def test_add_label(self):
        lid = db.add_label("🎵", "Music")
        label = db.get_label_by_name("Music")
        assert label is not None
        assert label["emoji"] == "🎵"
        assert label["id"] == lid

    def test_add_duplicate_label_raises(self):
        db.add_label("🎵", "UniqueLabel")
        with pytest.raises(Exception):
            db.add_label("🎶", "UniqueLabel")

    def test_get_label_by_name_case_insensitive(self):
        label = db.get_label_by_name("home")
        assert label is not None
        assert label["name"] == "Home"

    def test_get_label_by_name_not_found(self):
        assert db.get_label_by_name("nonexistent") is None

    def test_update_label(self):
        lid = db.add_label("🎵", "OldName")
        db.update_label(lid, emoji="🎶", name="NewName")
        label = db.get_label_by_name("NewName")
        assert label["emoji"] == "🎶"

    def test_delete_label(self):
        lid = db.add_label("🗑️", "ToDelete")
        db.delete_label(lid)
        assert db.get_label_by_name("ToDelete") is None

    def test_add_and_remove_task_label(self):
        tid = db.add_task("Task", "2026-03-10", None)
        label = db.get_label_by_name("Work")
        db.add_task_label(tid, label["id"])
        labels = db.get_labels_for_task(tid)
        assert any(l["name"] == "Work" for l in labels)

        db.remove_task_label(tid, label["id"])
        labels = db.get_labels_for_task(tid)
        assert not any(l["name"] == "Work" for l in labels)

    def test_add_task_label_idempotent(self):
        tid = db.add_task("Task", "2026-03-10", None)
        label = db.get_label_by_name("Work")
        db.add_task_label(tid, label["id"])
        db.add_task_label(tid, label["id"])  # no error
        labels = db.get_labels_for_task(tid)
        work_labels = [l for l in labels if l["name"] == "Work"]
        assert len(work_labels) == 1

    def test_get_labels_for_tasks_bulk(self):
        t1 = db.add_task("A", "2026-03-10", None)
        t2 = db.add_task("B", "2026-03-10", None)
        work = db.get_label_by_name("Work")
        home = db.get_label_by_name("Home")
        db.add_task_label(t1, work["id"])
        db.add_task_label(t2, home["id"])
        result = db.get_labels_for_tasks([t1, t2])
        assert len(result[t1]) == 1
        assert result[t1][0]["name"] == "Work"
        assert result[t2][0]["name"] == "Home"

    def test_get_labels_for_tasks_empty(self):
        assert db.get_labels_for_tasks([]) == {}

    def test_get_tasks_by_label(self):
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        t1 = db.add_task("Work task", today, None)
        t2 = db.add_task("Home task", today, None)
        work = db.get_label_by_name("Work")
        db.add_task_label(t1, work["id"])
        tasks = db.get_tasks_by_label(work["id"])
        assert len(tasks) == 1
        assert tasks[0]["description"] == "Work task"

    def test_delete_label_cascades_to_task_labels(self):
        tid = db.add_task("Task", "2026-03-10", None)
        lid = db.add_label("🧪", "TestLabel")
        db.add_task_label(tid, lid)
        assert len(db.get_labels_for_task(tid)) == 1
        db.delete_label(lid)
        assert len(db.get_labels_for_task(tid)) == 0
