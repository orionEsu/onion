import logging
import shutil
import sqlite3
from calendar import monthrange
from contextlib import contextmanager
from datetime import datetime, timedelta, date
from pathlib import Path
from bot.config import DB_PATH, TIMEZONE

logger = logging.getLogger(__name__)

VALID_STATUSES = {"pending", "done", "cancelled"}
db_startup_status = None  # Set by init_db: "ok", "restored", "fresh", or "awaiting_upload"

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    description  TEXT    NOT NULL,
    due_date     TEXT    NOT NULL,
    due_time     TEXT    DEFAULT NULL,
    status       TEXT    NOT NULL DEFAULT 'pending',
    reminder_24h INTEGER NOT NULL DEFAULT 0,
    reminder_2h  INTEGER NOT NULL DEFAULT 0,
    reviewed     INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT    NOT NULL
);
"""

DAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6,
           "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
           "friday": 4, "saturday": 5, "sunday": 6}


@contextmanager
def _conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _check_db_integrity() -> bool:
    """Return True if the database file exists and passes an integrity check."""
    if not DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(str(DB_PATH))
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        return result[0] == "ok"
    except Exception:
        return False


def _restore_from_backup() -> bool:
    """Attempt to restore the database from the backup file. Returns True on success."""
    backup_path = Path(str(DB_PATH) + ".bak")
    if not backup_path.exists():
        return False
    try:
        # Verify backup integrity before restoring
        conn = sqlite3.connect(str(backup_path))
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        if result[0] != "ok":
            logger.error("Backup file is also corrupted, cannot restore")
            return False
        shutil.copy2(str(backup_path), str(DB_PATH))
        logger.warning("Database restored from backup: %s", backup_path)
        return True
    except Exception as e:
        logger.error("Failed to restore from backup: %s", e)
        return False


def restore_from_upload(file_path: str) -> bool:
    """Replace the database with an uploaded backup file. Returns True on success."""
    global db_startup_status
    try:
        conn = sqlite3.connect(file_path)
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        if result[0] != "ok":
            return False
        shutil.copy2(file_path, str(DB_PATH))
        db_startup_status = "ok"
        init_db()  # Re-run migrations on restored DB
        return True
    except Exception as e:
        logger.error("Failed to restore from uploaded file: %s", e)
        return False


def init_db() -> None:
    global db_startup_status
    if DB_PATH.exists() and not _check_db_integrity():
        logger.error("Database corrupted, attempting restore from backup")
        if _restore_from_backup():
            logger.info("Database restored successfully")
            db_startup_status = "restored"
        else:
            logger.warning("No valid backup available, starting with fresh database")
            DB_PATH.unlink(missing_ok=True)
            db_startup_status = "awaiting_upload"
    elif db_startup_status is None:
        db_startup_status = "ok"

    with _conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)

        # --- Migrations ---
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "recurrence_rule" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN recurrence_rule TEXT DEFAULT NULL")
        if "recurrence_active" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN recurrence_active INTEGER NOT NULL DEFAULT 0")
        if "parent_task_id" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN parent_task_id INTEGER DEFAULT NULL")
        if "notes" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN notes TEXT DEFAULT NULL")
        if "scheduled_time" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN scheduled_time TEXT DEFAULT NULL")

        # Labels table
        conn.execute("""CREATE TABLE IF NOT EXISTS labels (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            emoji TEXT    NOT NULL,
            name  TEXT    NOT NULL UNIQUE
        )""")

        # Seed preset labels
        for emoji, name in [("🏠", "Home"), ("💼", "Work"), ("🏃", "Health"),
                            ("📚", "Learning"), ("🛒", "Errands")]:
            conn.execute("INSERT OR IGNORE INTO labels (emoji, name) VALUES (?, ?)", (emoji, name))

        # Junction table
        conn.execute("""CREATE TABLE IF NOT EXISTS task_labels (
            task_id  INTEGER NOT NULL,
            label_id INTEGER NOT NULL,
            PRIMARY KEY (task_id, label_id),
            FOREIGN KEY (task_id)  REFERENCES tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (label_id) REFERENCES labels(id) ON DELETE CASCADE
        )""")

        # Routine tables
        conn.execute("""CREATE TABLE IF NOT EXISTS routine_items (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            target_time TEXT DEFAULT NULL,
            sort_order  INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT NOT NULL
        )""")

        conn.execute("""CREATE TABLE IF NOT EXISTS routine_completions (
            routine_id     INTEGER NOT NULL,
            completed_date TEXT NOT NULL,
            PRIMARY KEY (routine_id, completed_date),
            FOREIGN KEY (routine_id) REFERENCES routine_items(id) ON DELETE CASCADE
        )""")

        # Flexible reminders table (replaces hardcoded reminder_24h / reminder_2h columns)
        conn.execute("""CREATE TABLE IF NOT EXISTS reminders_sent (
            task_id        INTEGER NOT NULL,
            offset_minutes INTEGER NOT NULL,
            PRIMARY KEY (task_id, offset_minutes),
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        )""")

        # Migrate old reminder flags to new table
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "reminder_24h" in columns:
            conn.execute(
                "INSERT OR IGNORE INTO reminders_sent (task_id, offset_minutes) "
                "SELECT id, 1440 FROM tasks WHERE reminder_24h = 1"
            )
            conn.execute(
                "INSERT OR IGNORE INTO reminders_sent (task_id, offset_minutes) "
                "SELECT id, 120 FROM tasks WHERE reminder_2h = 1"
            )

        # Per-task custom reminders
        conn.execute("""CREATE TABLE IF NOT EXISTS task_custom_reminders (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id          INTEGER NOT NULL,
            type             TEXT    NOT NULL,
            fire_at          TEXT    DEFAULT NULL,
            offset_minutes   INTEGER DEFAULT NULL,
            interval_minutes INTEGER DEFAULT NULL,
            last_fired_at    TEXT    DEFAULT NULL,
            fired            INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_reminders_task ON task_custom_reminders(task_id)")

        # Indexes for common queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_date_status ON tasks(due_date, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks(parent_task_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_labels_label ON task_labels(label_id)")


def _validate_recurrence_rule(rule: str | None) -> str | None:
    """Validate and return the rule, or None if invalid."""
    if not rule:
        return None
    if rule == "daily":
        return rule
    if rule.startswith("every_n_days:"):
        try:
            n = int(rule.split(":", 1)[1])
            return rule if n >= 1 else None
        except ValueError:
            return None
    if rule.startswith("weekly:") or rule.startswith("biweekly:"):
        day = rule.split(":", 1)[1].lower()
        return rule if day in DAY_MAP else None
    if rule.startswith("monthly:"):
        try:
            d = int(rule.split(":", 1)[1])
            return rule if 1 <= d <= 31 else None
        except ValueError:
            return None
    if rule.startswith("specific:"):
        days = rule.split(":", 1)[1].split(",")
        valid = [d.strip().lower() for d in days if d.strip().lower() in DAY_MAP]
        return rule if valid else None
    return None


# ── Task CRUD ──────────────────────────────────────────────────────

def add_task(description: str, due_date: str, due_time: str | None,
             recurrence_rule: str | None = None, parent_task_id: int | None = None,
             notes: str | None = None, scheduled_time: str | None = None) -> int:
    recurrence_rule = _validate_recurrence_rule(recurrence_rule)
    now = datetime.now(TIMEZONE).isoformat()
    active = 1 if recurrence_rule else 0
    # For recurring tasks, store the canonical scheduled time
    sched = scheduled_time or (due_time if recurrence_rule else None)
    with _conn() as conn:
        cur = conn.execute(
            "INSERT INTO tasks (description, due_date, due_time, recurrence_rule, "
            "recurrence_active, parent_task_id, notes, scheduled_time, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (description, due_date, due_time, recurrence_rule, active,
             parent_task_id, notes, sched, now),
        )
        return cur.lastrowid


def update_task_notes(task_id: int, notes: str | None) -> None:
    with _conn() as conn:
        conn.execute("UPDATE tasks SET notes = ? WHERE id = ?", (notes, task_id))


def get_task(task_id: int) -> sqlite3.Row | None:
    with _conn() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()


def get_tasks_for_date(target_date: str) -> list[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE due_date = ? AND status = 'pending' "
            "ORDER BY CASE WHEN due_time IS NULL THEN 1 ELSE 0 END, due_time",
            (target_date,),
        ).fetchall()


def get_upcoming_tasks() -> list[sqlite3.Row]:
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE due_date >= ? AND status = 'pending' "
            "ORDER BY due_date, CASE WHEN due_time IS NULL THEN 1 ELSE 0 END, due_time",
            (today,),
        ).fetchall()


def get_unreviewed_tasks_for_date(target_date: str) -> list[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE due_date = ? AND status = 'pending' AND reviewed = 0 "
            "ORDER BY CASE WHEN due_time IS NULL THEN 1 ELSE 0 END, due_time",
            (target_date,),
        ).fetchall()


def get_tasks_needing_reminder(offset_minutes: int, now: datetime) -> list[sqlite3.Row]:
    """Get pending tasks that need a reminder at the given offset (minutes before due)."""
    window_end = now + timedelta(minutes=offset_minutes + 30)

    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status = 'pending' AND due_time IS NOT NULL "
            "AND id NOT IN (SELECT task_id FROM reminders_sent WHERE offset_minutes = ?) "
            "AND (due_date || ' ' || due_time) <= ? "
            "AND (due_date || ' ' || due_time) > ?",
            (offset_minutes, window_end.strftime("%Y-%m-%d %H:%M"), now.strftime("%Y-%m-%d %H:%M")),
        ).fetchall()


def mark_reminder_sent(task_id: int, offset_minutes: int) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO reminders_sent (task_id, offset_minutes) VALUES (?, ?)",
            (task_id, offset_minutes),
        )


def clear_reminders_for_task(task_id: int) -> None:
    """Clear all sent reminders for a task (used when rescheduling)."""
    with _conn() as conn:
        conn.execute("DELETE FROM reminders_sent WHERE task_id = ?", (task_id,))
        conn.execute("DELETE FROM task_custom_reminders WHERE task_id = ?", (task_id,))


# ── Custom per-task reminders ────────────────────────────────────


def add_custom_reminder(
    task_id: int, type: str, *, fire_at: str | None = None,
    offset_minutes: int | None = None, interval_minutes: int | None = None,
) -> int:
    with _conn() as conn:
        cur = conn.execute(
            "INSERT INTO task_custom_reminders (task_id, type, fire_at, offset_minutes, interval_minutes) "
            "VALUES (?, ?, ?, ?, ?)",
            (task_id, type, fire_at, offset_minutes, interval_minutes),
        )
        return cur.lastrowid


def get_custom_reminders_for_task(task_id: int) -> list[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM task_custom_reminders WHERE task_id = ?", (task_id,),
        ).fetchall()


def get_pending_absolute_reminders(now: datetime) -> list[sqlite3.Row]:
    """Unfired absolute reminders whose fire_at time has been reached, for pending tasks."""
    with _conn() as conn:
        return conn.execute(
            "SELECT r.*, t.description, t.due_date, t.due_time "
            "FROM task_custom_reminders r JOIN tasks t ON r.task_id = t.id "
            "WHERE r.type = 'absolute' AND r.fired = 0 AND t.status = 'pending' "
            "AND r.fire_at <= ?",
            (now.strftime("%Y-%m-%d %H:%M"),),
        ).fetchall()


def get_pending_offset_reminders(now: datetime) -> list[sqlite3.Row]:
    """Unfired offset reminders where (due_time - offset) has been reached, for pending tasks with a due_time."""
    with _conn() as conn:
        return conn.execute(
            "SELECT r.*, t.description, t.due_date, t.due_time "
            "FROM task_custom_reminders r JOIN tasks t ON r.task_id = t.id "
            "WHERE r.type = 'offset' AND r.fired = 0 AND t.status = 'pending' "
            "AND t.due_time IS NOT NULL "
            "AND datetime(t.due_date || ' ' || t.due_time, '-' || r.offset_minutes || ' minutes') <= ?",
            (now.strftime("%Y-%m-%d %H:%M"),),
        ).fetchall()


def get_pending_repeating_reminders(now: datetime) -> list[sqlite3.Row]:
    """Repeating reminders that should fire: interval elapsed since last fire, and due time not yet passed."""
    now_str = now.strftime("%Y-%m-%d %H:%M")
    with _conn() as conn:
        return conn.execute(
            "SELECT r.*, t.description, t.due_date, t.due_time "
            "FROM task_custom_reminders r JOIN tasks t ON r.task_id = t.id "
            "WHERE r.type = 'repeating' AND r.fired = 0 AND t.status = 'pending' "
            "AND t.due_time IS NOT NULL "
            "AND (t.due_date || ' ' || t.due_time) > ? "
            "AND ("
            "  r.last_fired_at IS NULL "
            "  OR datetime(r.last_fired_at, '+' || r.interval_minutes || ' minutes') <= ?"
            ")",
            (now_str, now_str),
        ).fetchall()


def mark_custom_reminder_fired(reminder_id: int, fired_at: str | None = None) -> None:
    """For absolute/offset: set fired=1. For repeating: update last_fired_at."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT type FROM task_custom_reminders WHERE id = ?", (reminder_id,),
        ).fetchone()
        if not row:
            return
        if row["type"] == "repeating":
            ts = fired_at or datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
            conn.execute(
                "UPDATE task_custom_reminders SET last_fired_at = ? WHERE id = ?",
                (ts, reminder_id),
            )
        else:
            conn.execute(
                "UPDATE task_custom_reminders SET fired = 1 WHERE id = ?",
                (reminder_id,),
            )


def mark_repeating_reminder_done(reminder_id: int) -> None:
    """Mark a repeating reminder as fully done (no more fires)."""
    with _conn() as conn:
        conn.execute(
            "UPDATE task_custom_reminders SET fired = 1 WHERE id = ?",
            (reminder_id,),
        )


def mark_reviewed(task_id: int) -> None:
    with _conn() as conn:
        conn.execute("UPDATE tasks SET reviewed = 1 WHERE id = ?", (task_id,))


def update_task_status(task_id: int, status: str) -> None:
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status!r}")
    with _conn() as conn:
        conn.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))


def carry_over_task(task_id: int, new_date: str, new_time: str | None) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE tasks SET due_date = ?, due_time = ?, reviewed = 0 WHERE id = ?",
            (new_date, new_time, task_id),
        )
    clear_reminders_for_task(task_id)


def delete_task(task_id: int) -> None:
    with _conn() as conn:
        conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))


def update_task(task_id: int, description: str | None = None,
                due_date: str | None = None, due_time: str | None = None) -> bool:
    """Update task fields. Resets reminder flags if date/time changed. Returns True if task exists."""
    sets = []
    params = []
    reset_reminders = False
    if description is not None:
        sets.append("description = ?")
        params.append(description)
    if due_date is not None:
        sets.append("due_date = ?")
        params.append(due_date)
        sets.append("reviewed = 0")
        reset_reminders = True
    if due_time is not None:
        sets.append("due_time = ?")
        params.append(due_time)
        reset_reminders = True
        # If this is a recurring task, also update the canonical scheduled_time
        task = get_task(task_id)
        if task and task["recurrence_rule"]:
            sets.append("scheduled_time = ?")
            params.append(due_time)
    if not sets:
        return False
    params.append(task_id)
    with _conn() as conn:
        cur = conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", params)
        updated = cur.rowcount > 0
    if updated and reset_reminders:
        clear_reminders_for_task(task_id)
    return updated


def get_overdue_tasks() -> list[sqlite3.Row]:
    """Get pending tasks where due_date is before today."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE due_date < ? AND status = 'pending' "
            "ORDER BY due_date, CASE WHEN due_time IS NULL THEN 1 ELSE 0 END, due_time",
            (today,),
        ).fetchall()


def get_tasks_in_date_range(start_date: str, end_date: str) -> list[sqlite3.Row]:
    """All tasks (any status) within a date range."""
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE due_date >= ? AND due_date <= ? "
            "ORDER BY due_date, CASE WHEN due_time IS NULL THEN 1 ELSE 0 END, due_time",
            (start_date, end_date),
        ).fetchall()


def get_weekly_stats(start_date: str, end_date: str) -> dict:
    """Return counts by status for a date range."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM tasks "
            "WHERE due_date >= ? AND due_date <= ? GROUP BY status",
            (start_date, end_date),
        ).fetchall()
    stats = {"done": 0, "pending": 0, "cancelled": 0}
    for row in rows:
        stats[row["status"]] = row["count"]
    return stats


def reinsert_task(task_data: dict) -> int:
    """Re-insert a previously deleted task with its original ID."""
    with _conn() as conn:
        conn.execute(
            "INSERT INTO tasks (id, description, due_date, due_time, status, "
            "reminder_24h, reminder_2h, reviewed, created_at, recurrence_rule, "
            "recurrence_active, parent_task_id, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_data["id"], task_data["description"], task_data["due_date"],
             task_data["due_time"], task_data["status"], task_data["reminder_24h"],
             task_data["reminder_2h"], task_data["reviewed"], task_data["created_at"],
             task_data["recurrence_rule"], task_data["recurrence_active"],
             task_data["parent_task_id"], task_data["notes"]),
        )
        return task_data["id"]


def clear_tasks(scope: str) -> int:
    """Delete tasks/labels by scope. Returns count of deleted items."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    with _conn() as conn:
        if scope == "today":
            cur = conn.execute("DELETE FROM tasks WHERE due_date = ? AND status = 'pending'", (today,))
            return cur.rowcount
        elif scope == "upcoming":
            cur = conn.execute("DELETE FROM tasks WHERE due_date >= ? AND status = 'pending'", (today,))
            return cur.rowcount
        elif scope == "all_tasks":
            cur = conn.execute("DELETE FROM tasks")
            conn.execute("DELETE FROM sqlite_sequence WHERE name='tasks'")
            return cur.rowcount
        elif scope == "all_labels":
            cur = conn.execute("DELETE FROM labels")
            conn.execute("DELETE FROM task_labels")
            return cur.rowcount
        elif scope == "everything":
            t = conn.execute("DELETE FROM tasks").rowcount
            l = conn.execute("DELETE FROM labels").rowcount
            conn.execute("DELETE FROM task_labels")
            conn.execute("DELETE FROM sqlite_sequence WHERE name='tasks'")
            return t + l
        else:
            return 0


def clear_tasks_except(scope: str, exclude_ids: set[int]) -> int:
    """Delete tasks by scope, excluding specific task IDs. Returns count of deleted items."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    if not exclude_ids:
        return clear_tasks(scope)
    placeholders = ",".join("?" for _ in exclude_ids)
    with _conn() as conn:
        if scope == "today":
            cur = conn.execute(
                f"DELETE FROM tasks WHERE due_date = ? AND status = 'pending' AND id NOT IN ({placeholders})",
                (today, *exclude_ids),
            )
            return cur.rowcount
        elif scope == "upcoming":
            cur = conn.execute(
                f"DELETE FROM tasks WHERE due_date >= ? AND status = 'pending' AND id NOT IN ({placeholders})",
                (today, *exclude_ids),
            )
            return cur.rowcount
        else:
            # For all_tasks/all_labels/everything, exclusion not supported — fall back
            return clear_tasks(scope)


def find_tasks_by_description(query: str) -> list[sqlite3.Row]:
    """Find pending tasks by partial description match (case-insensitive)."""
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status = 'pending' AND LOWER(description) LIKE ? "
            "ORDER BY due_date",
            (f"%{query.lower()}%",),
        ).fetchall()


def find_done_tasks_by_description(query: str) -> list[sqlite3.Row]:
    """Find recently completed tasks by partial description match (case-insensitive)."""
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status = 'done' AND LOWER(description) LIKE ? "
            "ORDER BY due_date DESC LIMIT 5",
            (f"%{query.lower()}%",),
        ).fetchall()


def get_completed_tasks_for_date(target_date: str) -> list[sqlite3.Row]:
    """Tasks with status='done' that were due on target_date."""
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE due_date = ? AND status = 'done' ORDER BY due_time",
            (target_date,),
        ).fetchall()


def get_completed_tasks(start_date: str | None = None,
                        end_date: str | None = None) -> list[sqlite3.Row]:
    """Completed tasks in a date range, ordered by due_date DESC."""
    query = "SELECT * FROM tasks WHERE status = 'done'"
    params = []
    if start_date:
        query += " AND due_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND due_date <= ?"
        params.append(end_date)
    query += " ORDER BY due_date DESC, due_time DESC"
    with _conn() as conn:
        return conn.execute(query, params).fetchall()


def get_all_tasks_in_range(start_date: str | None = None,
                           end_date: str | None = None) -> list[sqlite3.Row]:
    """All tasks (done, cancelled, pending) in a date range, ordered by due_date DESC."""
    query = "SELECT * FROM tasks WHERE 1=1"
    params = []
    if start_date:
        query += " AND due_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND due_date <= ?"
        params.append(end_date)
    query += " ORDER BY due_date DESC, due_time DESC"
    with _conn() as conn:
        return conn.execute(query, params).fetchall()


# ── Recurrence ─────────────────────────────────────────────────────

def compute_next_date(current_date: str, rule: str) -> str | None:
    """Compute the next occurrence date from a recurrence rule. Returns None if rule is invalid."""
    try:
        d = date.fromisoformat(current_date)
    except ValueError:
        return None

    if rule == "daily":
        return (d + timedelta(days=1)).isoformat()

    if rule.startswith("every_n_days:"):
        try:
            n = int(rule.split(":", 1)[1])
            if n < 1:
                return None
        except ValueError:
            return None
        return (d + timedelta(days=n)).isoformat()

    if rule.startswith("weekly:"):
        day_name = rule.split(":", 1)[1].lower()
        if day_name not in DAY_MAP:
            return None
        target_dow = DAY_MAP[day_name]
        days_ahead = (target_dow - d.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        return (d + timedelta(days=days_ahead)).isoformat()

    if rule.startswith("biweekly:"):
        day_name = rule.split(":", 1)[1].lower()
        if day_name not in DAY_MAP:
            return None
        target_dow = DAY_MAP[day_name]
        days_ahead = (target_dow - d.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        next_d = d + timedelta(days=days_ahead)
        if (next_d - d).days < 14:
            next_d += timedelta(weeks=1)
        return next_d.isoformat()

    if rule.startswith("monthly:"):
        try:
            target_day = int(rule.split(":", 1)[1])
        except ValueError:
            return None
        year, month = d.year, d.month
        # Always advance to next month if today >= target day
        if d.day >= target_day:
            month += 1
            if month > 12:
                month = 1
                year += 1
        max_day = monthrange(year, month)[1]
        actual_day = min(target_day, max_day)
        return date(year, month, actual_day).isoformat()

    if rule.startswith("specific:"):
        day_names = rule.split(":", 1)[1].split(",")
        target_dows = set()
        for dn in day_names:
            dn = dn.strip().lower()
            if dn in DAY_MAP:
                target_dows.add(DAY_MAP[dn])
        if not target_dows:
            return None
        for i in range(1, 8):
            candidate = d + timedelta(days=i)
            if candidate.weekday() in target_dows:
                return candidate.isoformat()

    return None


def create_next_occurrence(task_id: int) -> int | None:
    """Create the next instance of a recurring task. Returns new task ID or None."""
    task = get_task(task_id)
    if not task or not task["recurrence_rule"] or not task["recurrence_active"]:
        return None

    next_date = compute_next_date(task["due_date"], task["recurrence_rule"])
    if not next_date:
        return None
    parent = task["parent_task_id"] or task_id

    # Use scheduled_time (original recurring time) if available, fall back to due_time
    original_time = task["scheduled_time"] or task["due_time"]
    new_id = add_task(
        description=task["description"],
        due_date=next_date,
        due_time=original_time,
        recurrence_rule=task["recurrence_rule"],
        parent_task_id=parent,
        notes=task["notes"],
        scheduled_time=original_time,
    )

    # Copy labels from the original task in one query
    with _conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO task_labels (task_id, label_id) "
            "SELECT ?, label_id FROM task_labels WHERE task_id = ?",
            (new_id, task_id),
        )

    return new_id


def stop_recurrence(task_id: int) -> None:
    """Stop a recurrence chain."""
    task = get_task(task_id)
    if not task:
        return
    parent = task["parent_task_id"] or task_id
    with _conn() as conn:
        conn.execute(
            "UPDATE tasks SET recurrence_active = 0, recurrence_rule = NULL "
            "WHERE id = ? OR parent_task_id = ?",
            (parent, parent),
        )


def _rule_matches_date(rule: str, last_date: str, target: str) -> bool:
    """Check if a recurrence rule would land on target date starting from last_date, without iterating."""
    try:
        d_last = date.fromisoformat(last_date)
        d_target = date.fromisoformat(target)
    except ValueError:
        return False

    if d_target <= d_last:
        return False

    if rule == "daily":
        return True  # daily always hits every date after last

    if rule.startswith("every_n_days:"):
        try:
            n = int(rule.split(":", 1)[1])
            if n < 1:
                return False
        except ValueError:
            return False
        return (d_target - d_last).days % n == 0

    if rule.startswith("weekly:"):
        day_name = rule.split(":", 1)[1].lower()
        if day_name not in DAY_MAP:
            return False
        return d_target.weekday() == DAY_MAP[day_name]

    if rule.startswith("biweekly:"):
        day_name = rule.split(":", 1)[1].lower()
        if day_name not in DAY_MAP:
            return False
        if d_target.weekday() != DAY_MAP[day_name]:
            return False
        weeks_diff = (d_target - d_last).days // 7
        return weeks_diff >= 2 and weeks_diff % 2 == 0

    if rule.startswith("monthly:"):
        try:
            target_day = int(rule.split(":", 1)[1])
        except ValueError:
            return False
        max_day = monthrange(d_target.year, d_target.month)[1]
        return d_target.day == min(target_day, max_day)

    if rule.startswith("specific:"):
        day_names = rule.split(":", 1)[1].split(",")
        target_dows = set()
        for dn in day_names:
            dn = dn.strip().lower()
            if dn in DAY_MAP:
                target_dows.add(DAY_MAP[dn])
        return d_target.weekday() in target_dows

    return False


def generate_recurring_for_today() -> list[int]:
    """Auto-generate today's instances of recurring tasks if they don't exist yet."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    created_ids = []

    with _conn() as conn:
        # Find the latest instance of each active recurring chain
        recurring = conn.execute(
            "SELECT * FROM tasks WHERE recurrence_active = 1 AND recurrence_rule IS NOT NULL "
            "ORDER BY due_date DESC"
        ).fetchall()

        seen_parents = set()
        for task in recurring:
            parent = task["parent_task_id"] or task["id"]
            if parent in seen_parents:
                continue
            seen_parents.add(parent)

            # Check if there's already a pending task for today or later in this chain
            existing = conn.execute(
                "SELECT id FROM tasks WHERE (id = ? OR parent_task_id = ?) "
                "AND due_date >= ? AND status = 'pending'",
                (parent, parent, today),
            ).fetchone()

            if existing:
                continue

            # Check if this task's rule would produce today (O(1) instead of iterating)
            if task["recurrence_rule"] and task["due_date"] < today:
                if not _rule_matches_date(task["recurrence_rule"], task["due_date"], today):
                    continue

                now = datetime.now(TIMEZONE).isoformat()
                active = 1
                cur = conn.execute(
                    "INSERT INTO tasks (description, due_date, due_time, recurrence_rule, "
                    "recurrence_active, parent_task_id, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (task["description"], today, task["due_time"], task["recurrence_rule"],
                     active, parent, task["notes"], now),
                )
                new_id = cur.lastrowid

                # Copy labels in one query
                conn.execute(
                    "INSERT OR IGNORE INTO task_labels (task_id, label_id) "
                    "SELECT ?, label_id FROM task_labels WHERE task_id = ?",
                    (new_id, task["id"]),
                )
                created_ids.append(new_id)

    return created_ids


# ── Labels ─────────────────────────────────────────────────────────

def get_all_labels() -> list[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute("SELECT * FROM labels ORDER BY id").fetchall()


def get_label_by_name(name: str) -> sqlite3.Row | None:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM labels WHERE LOWER(name) = LOWER(?)", (name,)
        ).fetchone()


def add_label(emoji: str, name: str) -> int:
    with _conn() as conn:
        cur = conn.execute("INSERT INTO labels (emoji, name) VALUES (?, ?)", (emoji, name))
        return cur.lastrowid


def update_label(label_id: int, emoji: str | None = None, name: str | None = None) -> None:
    with _conn() as conn:
        if emoji is not None:
            conn.execute("UPDATE labels SET emoji = ? WHERE id = ?", (emoji, label_id))
        if name is not None:
            conn.execute("UPDATE labels SET name = ? WHERE id = ?", (name, label_id))


def delete_label(label_id: int) -> None:
    with _conn() as conn:
        conn.execute("DELETE FROM labels WHERE id = ?", (label_id,))


def add_task_label(task_id: int, label_id: int) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO task_labels (task_id, label_id) VALUES (?, ?)",
            (task_id, label_id),
        )


def remove_task_label(task_id: int, label_id: int) -> None:
    with _conn() as conn:
        conn.execute(
            "DELETE FROM task_labels WHERE task_id = ? AND label_id = ?",
            (task_id, label_id),
        )


def get_labels_for_task(task_id: int) -> list[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute(
            "SELECT l.* FROM labels l JOIN task_labels tl ON l.id = tl.label_id "
            "WHERE tl.task_id = ?", (task_id,)
        ).fetchall()


def get_labels_for_tasks(task_ids: list[int]) -> dict[int, list[sqlite3.Row]]:
    """Bulk fetch labels for multiple tasks in a single query. Returns {task_id: [label_rows]}."""
    if not task_ids:
        return {}
    placeholders = ",".join("?" for _ in task_ids)
    with _conn() as conn:
        rows = conn.execute(
            f"SELECT tl.task_id, l.* FROM labels l JOIN task_labels tl ON l.id = tl.label_id "
            f"WHERE tl.task_id IN ({placeholders})", task_ids,
        ).fetchall()
    result = {tid: [] for tid in task_ids}
    for row in rows:
        result[row["task_id"]].append(row)
    return result


def get_tasks_by_label(label_id: int) -> list[sqlite3.Row]:
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    with _conn() as conn:
        return conn.execute(
            "SELECT t.* FROM tasks t JOIN task_labels tl ON t.id = tl.task_id "
            "WHERE tl.label_id = ? AND t.status = 'pending' AND t.due_date >= ? "
            "ORDER BY t.due_date, t.due_time", (label_id, today),
        ).fetchall()


# ── Routine ───────────────────────────────────────────────────────

def add_routine_item(description: str, target_time: str | None = None) -> int:
    now = datetime.now(TIMEZONE).isoformat()
    with _conn() as conn:
        max_order = conn.execute("SELECT COALESCE(MAX(sort_order), 0) FROM routine_items").fetchone()[0]
        cur = conn.execute(
            "INSERT INTO routine_items (description, target_time, sort_order, created_at) VALUES (?, ?, ?, ?)",
            (description, target_time, max_order + 1, now),
        )
        return cur.lastrowid


def get_all_routine_items() -> list[sqlite3.Row]:
    with _conn() as conn:
        return conn.execute("SELECT * FROM routine_items ORDER BY sort_order, id").fetchall()


def get_routine_item(item_id: int) -> sqlite3.Row | None:
    with _conn() as conn:
        return conn.execute("SELECT * FROM routine_items WHERE id = ?", (item_id,)).fetchone()


def get_routine_item_by_description(query: str) -> sqlite3.Row | None:
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM routine_items WHERE LOWER(description) LIKE ? ORDER BY sort_order LIMIT 1",
            (f"%{query.lower()}%",),
        ).fetchone()


def delete_routine_item(item_id: int) -> bool:
    with _conn() as conn:
        cur = conn.execute("DELETE FROM routine_items WHERE id = ?", (item_id,))
        return cur.rowcount > 0


def complete_routine_item(routine_id: int, date_str: str) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO routine_completions (routine_id, completed_date) VALUES (?, ?)",
            (routine_id, date_str),
        )


def uncomplete_routine_item(routine_id: int, date_str: str) -> None:
    with _conn() as conn:
        conn.execute(
            "DELETE FROM routine_completions WHERE routine_id = ? AND completed_date = ?",
            (routine_id, date_str),
        )


def get_routine_completions_for_date(date_str: str) -> set[int]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT routine_id FROM routine_completions WHERE completed_date = ?", (date_str,),
        ).fetchall()
        return {row["routine_id"] for row in rows}


def is_routine_all_complete(date_str: str) -> bool:
    with _conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM routine_items").fetchone()[0]
        if total == 0:
            return False
        done = conn.execute(
            "SELECT COUNT(*) FROM routine_completions WHERE completed_date = ?", (date_str,),
        ).fetchone()[0]
        return done >= total


def get_week_task_counts(start_date: str) -> dict[str, int]:
    d = date.fromisoformat(start_date)
    end = d + timedelta(days=6)
    with _conn() as conn:
        rows = conn.execute(
            "SELECT due_date, COUNT(*) as cnt FROM tasks "
            "WHERE due_date >= ? AND due_date <= ? AND status = 'pending' "
            "GROUP BY due_date",
            (start_date, end.isoformat()),
        ).fetchall()
    return {row["due_date"]: row["cnt"] for row in rows}


