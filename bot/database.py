import sqlite3
from calendar import monthrange
from contextlib import contextmanager
from datetime import datetime, timedelta, date
from bot.config import DB_PATH, TIMEZONE

VALID_STATUSES = {"pending", "done", "cancelled"}
_REMINDER_COLS = {"24h": "reminder_24h", "2h": "reminder_2h"}

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
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _conn() as conn:
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


# ── Task CRUD ──────────────────────────────────────────────────────

def add_task(description: str, due_date: str, due_time: str | None,
             recurrence_rule: str | None = None, parent_task_id: int | None = None,
             notes: str | None = None) -> int:
    now = datetime.now(TIMEZONE).isoformat()
    active = 1 if recurrence_rule else 0
    with _conn() as conn:
        cur = conn.execute(
            "INSERT INTO tasks (description, due_date, due_time, recurrence_rule, "
            "recurrence_active, parent_task_id, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (description, due_date, due_time, recurrence_rule, active, parent_task_id, notes, now),
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


def get_tasks_needing_reminder(reminder_type: str, now: datetime) -> list[sqlite3.Row]:
    flag_col = _REMINDER_COLS.get(reminder_type)
    if not flag_col:
        raise ValueError(f"Invalid reminder_type: {reminder_type!r}")
    minutes = 1440 if reminder_type == "24h" else 120
    window_end = now + timedelta(minutes=minutes + 30)

    with _conn() as conn:
        return conn.execute(
            f"SELECT * FROM tasks WHERE status = 'pending' AND due_time IS NOT NULL "
            f"AND {flag_col} = 0 "
            f"AND (due_date || ' ' || due_time) <= ? "
            f"AND (due_date || ' ' || due_time) > ?",
            (window_end.strftime("%Y-%m-%d %H:%M"), now.strftime("%Y-%m-%d %H:%M")),
        ).fetchall()


def mark_reminder_sent(task_id: int, reminder_type: str) -> None:
    flag_col = _REMINDER_COLS.get(reminder_type)
    if not flag_col:
        raise ValueError(f"Invalid reminder_type: {reminder_type!r}")
    with _conn() as conn:
        conn.execute(f"UPDATE tasks SET {flag_col} = 1 WHERE id = ?", (task_id,))


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
            "UPDATE tasks SET due_date = ?, due_time = ?, "
            "reminder_24h = 0, reminder_2h = 0, reviewed = 0 WHERE id = ?",
            (new_date, new_time, task_id),
        )


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
    if reset_reminders:
        sets.extend(["reminder_24h = 0", "reminder_2h = 0"])
    if not sets:
        return False
    params.append(task_id)
    with _conn() as conn:
        cur = conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE id = ?", params)
        return cur.rowcount > 0


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


def find_tasks_by_description(query: str) -> list[sqlite3.Row]:
    """Find pending tasks by partial description match (case-insensitive)."""
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM tasks WHERE status = 'pending' AND LOWER(description) LIKE ? "
            "ORDER BY due_date",
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

    new_id = add_task(
        description=task["description"],
        due_date=next_date,
        due_time=task["due_time"],
        recurrence_rule=task["recurrence_rule"],
        parent_task_id=parent,
        notes=task["notes"],
    )

    # Copy labels from the original task
    with _conn() as conn:
        labels = conn.execute(
            "SELECT label_id FROM task_labels WHERE task_id = ?", (task_id,)
        ).fetchall()
        for lbl in labels:
            conn.execute(
                "INSERT OR IGNORE INTO task_labels (task_id, label_id) VALUES (?, ?)",
                (new_id, lbl["label_id"]),
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

                # Copy labels within the same connection
                labels = conn.execute(
                    "SELECT label_id FROM task_labels WHERE task_id = ?", (task["id"],)
                ).fetchall()
                for lbl in labels:
                    conn.execute(
                        "INSERT OR IGNORE INTO task_labels (task_id, label_id) VALUES (?, ?)",
                        (new_id, lbl["label_id"]),
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
