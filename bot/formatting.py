"""Styled HTML message formatting with emojis for all bot outputs."""

import random
from datetime import datetime, timedelta, date
from html import escape

from bot.config import TIMEZONE

# ── Morning greetings (rotated) ───────────────────────────────────

MORNING_GREETINGS = [
    "☀️ <b>Rise and shine!</b> A new day, a new chance to crush it.",
    "🌅 <b>Good morning!</b> The world is yours today.",
    "☕ <b>Morning!</b> Coffee's calling and so is productivity.",
    "🚀 <b>New day loading...</b> What quests are we taking on?",
    "🌤️ <b>Top of the morning!</b> Let's make today count.",
    "💪 <b>Wakey wakey!</b> Time to be legendary.",
    "🌻 <b>Hello sunshine!</b> What's the game plan?",
    "⭐ <b>A fresh start!</b> Yesterday is gone, today is yours.",
    "🎯 <b>Good morning!</b> Let's aim high today.",
    "🔥 <b>Let's gooo!</b> Another day to make moves.",
    "🌈 <b>Rise up!</b> Great things await.",
    "📍 <b>Morning check-in!</b> Ready to own this day?",
    "✨ <b>Hey there, superstar!</b> What's on the agenda?",
    "🎶 <b>Good morning!</b> Time to set the rhythm for the day.",
    "🧭 <b>New day, new direction!</b> Where are we headed?",
]


def format_morning_prompt() -> str:
    greeting = random.choice(MORNING_GREETINGS)
    return (
        f"{greeting}\n\n"
        f"📝 <b>What are you up to today?</b>\n"
        f"<i>Send me your tasks and I'll add them. Tap the button when you're done.</i>"
    )


def format_morning_summary(tasks_added: list, all_tasks: list) -> str:
    lines = [f"📊 <b>Morning Planning Done!</b>\n"]
    if tasks_added:
        lines.append(f"✅ Added <b>{len(tasks_added)}</b> new task(s)")
    lines.append(f"📋 Total tasks for today: <b>{len(all_tasks)}</b>\n")
    if all_tasks:
        for t in all_tasks:
            lines.append(format_task_line(t))
    lines.append("\n💪 <i>Let's get it done!</i>")
    return "\n".join(lines)


# ── Task formatting ───────────────────────────────────────────────

def _humanize_rule(rule: str) -> str:
    """Convert a recurrence rule to human-readable text."""
    if rule == "daily":
        return "every day"
    if rule.startswith("every_n_days:"):
        n = rule.split(":", 1)[1]
        return f"every {n} days"
    if rule.startswith("weekly:"):
        day = rule.split(":", 1)[1].capitalize()
        return f"every {day}"
    if rule.startswith("biweekly:"):
        day = rule.split(":", 1)[1].capitalize()
        return f"every other {day}"
    if rule.startswith("monthly:"):
        day = rule.split(":", 1)[1]
        d = int(day)
        if d % 10 == 1 and d != 11:
            suffix = "st"
        elif d % 10 == 2 and d != 12:
            suffix = "nd"
        elif d % 10 == 3 and d != 13:
            suffix = "rd"
        else:
            suffix = "th"
        return f"monthly on the {d}{suffix}"
    if rule.startswith("specific:"):
        days = rule.split(":", 1)[1]
        day_names = [d.strip().capitalize() for d in days.split(",")]
        return "every " + ", ".join(day_names)
    return rule


DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _humanize_date(due_date: str) -> str:
    """Convert a date string to a human-readable relative label."""
    try:
        d = date.fromisoformat(due_date)
    except ValueError:
        return due_date

    today = datetime.now(TIMEZONE).date()
    delta = (d - today).days

    if delta == 0:
        return "today"
    if delta == 1:
        return "tomorrow"
    if delta == -1:
        return "yesterday"

    day_name = DAY_NAMES[d.weekday()]

    # Within this week (2-6 days ahead)
    if 2 <= delta <= 6:
        return day_name

    # Next week (7-13 days ahead)
    if 7 <= delta <= 13:
        return f"next {day_name}"

    # Same month, more than a week away
    if d.month == today.month and d.year == today.year:
        return f"{day_name}, {d.strftime('%B')} {d.day}"

    # Different month
    return f"{day_name}, {d.strftime('%B')} {d.day}"


def _recurrence_badge(rule: str | None) -> str:
    if not rule:
        return ""
    return " 🔄"


def _label_badges(labels: list | None) -> str:
    if not labels:
        return ""
    return " " + " ".join(f"{l['emoji']}" for l in labels)


def _safe_get(task, key):
    """Safely get a value from a sqlite3.Row or dict."""
    try:
        val = task[key]
        return val
    except (KeyError, IndexError):
        return None


def format_task_line(task, labels: list | None = None, show_date: bool = False,
                     position: int | None = None) -> str:
    status = _safe_get(task, "status") or "pending"
    if status == "done":
        icon = "✅"
    elif labels:
        icon = labels[0]["emoji"]
    else:
        icon = "⏳"
    desc = escape(task["description"])
    time_str = f" at {task['due_time']}" if _safe_get(task, "due_time") else ""
    date_str = f" — {_humanize_date(task['due_date'])}" if show_date else ""
    recur = _recurrence_badge(_safe_get(task, "recurrence_rule"))
    # Show remaining label emojis if more than one
    extra_lbls = " ".join(l["emoji"] for l in labels[1:]) if labels and len(labels) > 1 else ""
    if extra_lbls:
        extra_lbls = " " + extra_lbls

    num = position if position is not None else task["id"]
    return f"{icon} <b>{num}.</b> {desc}{time_str}{date_str}{recur}{extra_lbls}"


def format_task_list(title: str, tasks: list, labels_map: dict | None = None,
                     show_date: bool = False) -> tuple[str, dict[int, int]]:
    """Returns (formatted_text, {position: task_id} mapping)."""
    if not tasks:
        return f"{title}\n\n🎉 <i>Nothing here! Enjoy the free time.</i>", {}

    lines = [f"{title}\n"]
    pos_map = {}
    for i, t in enumerate(tasks, 1):
        task_labels = labels_map.get(t["id"]) if labels_map else None
        lines.append(format_task_line(t, labels=task_labels, show_date=show_date, position=i))
        pos_map[i] = t["id"]
    return "\n".join(lines), pos_map


def format_task_added(task_id: int, description: str, due_date: str,
                      due_time: str | None, recurrence_rule: str | None,
                      labels: list | None = None, notes: str | None = None) -> str:
    desc = escape(description)
    time_str = f" at {due_time}" if due_time else ""
    recur_str = ""
    if recurrence_rule:
        recur_str = f"\n🔄 Repeats: {_humanize_rule(recurrence_rule)}"
    label_str = ""
    if labels:
        label_str = "\n🏷️ " + " ".join(f"{l['emoji']} {l['name']}" for l in labels)
    notes_str = ""
    if notes:
        notes_str = f"\n📎 <i>{escape(notes)}</i>"

    return (
        f"✅ <b>Added!</b>\n\n"
        f"📝 {desc}\n"
        f"📅 {_humanize_date(due_date)}{time_str}"
        f"{recur_str}{label_str}{notes_str}"
    )


def format_task_detail(task, labels: list | None = None) -> str:
    """Full detail view of a single task."""
    status_icon = "✅" if task["status"] == "done" else "⏳"
    desc = escape(task["description"])
    time_str = f" at {task['due_time']}" if _safe_get(task, "due_time") else ""
    recur = ""
    rule = _safe_get(task, "recurrence_rule")
    if rule:
        recur = f"\n🔄 Repeats: {_humanize_rule(rule)}"
    label_str = ""
    if labels:
        label_str = "\n🏷️ " + " ".join(f"{l['emoji']} {l['name']}" for l in labels)
    notes = _safe_get(task, "notes")
    notes_str = f"\n\n📎 <b>Notes:</b>\n<i>{escape(notes)}</i>" if notes else "\n\n📎 <i>No notes</i>"

    return (
        f"{status_icon} <b>{desc}</b>\n\n"
        f"📅 {_humanize_date(task['due_date'])}{time_str}\n"
        f"📊 Status: {task['status']}"
        f"{recur}{label_str}{notes_str}"
    )


def format_task_done(task, next_task_id: int | None = None) -> str:
    desc = escape(task["description"])
    msg = f"🎉 <b>Done!</b> ✅ <s>{desc}</s>"
    if next_task_id:
        next_task = None
        # next_task_id info will be appended by caller if needed
        msg += f"\n\n🔄 Next occurrence scheduled"
    return msg


# ── Review ─────────────────────────────────────────────────────────

def format_daily_review_header() -> str:
    return (
        "🌙 <b>End-of-Day Review</b>\n\n"
        "Let's see what you got done today! Tap the buttons below for each task."
    )


def format_review_task(task, labels: list | None = None) -> str:
    desc = escape(task["description"])
    time_str = f" ({task['due_time']})" if _safe_get(task, "due_time") else ""
    lbls = _label_badges(labels)
    return f"📌 <b>#{task['id']}</b>. {desc}{time_str}{lbls}"


def format_review_done(task_id: int) -> str:
    return f"✅ <b>#{task_id}</b> — Done! Great job! 🎉"


def format_review_carried(task_id: int, new_date: str) -> str:
    return f"📦 <b>#{task_id}</b> — Moved to {_humanize_date(new_date)}"


def format_review_dropped(task_id: int) -> str:
    return f"🗑️ <b>#{task_id}</b> — Dropped"


def format_no_tasks_review() -> str:
    return "🌙 <b>Daily Review</b>\n\n🎉 No pending tasks for today. <i>Nice work!</i>"


# ── Reminders ──────────────────────────────────────────────────────

def format_reminder(task, time_label: str, labels: list | None = None) -> str:
    desc = escape(task["description"])
    lbls = _label_badges(labels)
    return (
        f"🔔 <b>Reminder!</b>\n\n"
        f"📝 \"{desc}\" is due in ~<b>{time_label}</b>\n"
        f"📅 {_humanize_date(task['due_date'])} at {task['due_time']}{lbls}"
    )


def format_custom_reminder_set(task, reminder_type: str, **kwargs) -> str:
    desc = escape(task["description"])
    if reminder_type == "absolute":
        fire_at = kwargs.get("fire_at", "")
        # Parse "YYYY-MM-DD HH:MM" to readable format
        try:
            dt = datetime.strptime(fire_at, "%Y-%m-%d %H:%M")
            time_str = dt.strftime("%H:%M")
            date_str = _humanize_date(dt.strftime("%Y-%m-%d"))
            when = f"at <b>{time_str}</b> {date_str}"
        except (ValueError, TypeError):
            when = f"at {fire_at}"
        return f"🔔 <b>Reminder set</b> for \"{desc}\" — {when}"
    elif reminder_type == "offset":
        mins = kwargs.get("offset_minutes", 0)
        if mins >= 60:
            h = mins // 60
            m = mins % 60
            label = f"{h}h {m}m" if m else f"{h} hour{'s' if h > 1 else ''}"
        else:
            label = f"{mins} minute{'s' if mins != 1 else ''}"
        return f"🔔 <b>Reminder set</b> for \"{desc}\" — <b>{label}</b> before due time"
    elif reminder_type == "repeating":
        mins = kwargs.get("interval_minutes", 0)
        if mins >= 60:
            h = mins // 60
            m = mins % 60
            label = f"{h}h {m}m" if m else f"{h} hour{'s' if h > 1 else ''}"
        else:
            label = f"{mins} minute{'s' if mins != 1 else ''}"
        return f"🔔 <b>Reminder set</b> for \"{desc}\" — every <b>{label}</b> until due"
    return f"🔔 <b>Reminder set</b> for \"{desc}\""


def format_custom_reminder_notification(task, reminder_type: str, labels: list | None = None) -> str:
    desc = escape(task["description"])
    lbls = _label_badges(labels)
    due_time = task["due_time"]
    if due_time:
        due_info = f"\n📅 {_humanize_date(task['due_date'])} at {due_time}{lbls}"
    else:
        due_info = f"\n📅 {_humanize_date(task['due_date'])}{lbls}"
    prefix = "🔔 <b>Custom Reminder!</b>"
    return f"{prefix}\n\n📝 \"{desc}\"{due_info}"


# ── Labels ─────────────────────────────────────────────────────────

def format_labels_list(labels: list) -> str:
    if not labels:
        return "🏷️ <b>Labels</b>\n\n<i>No labels yet. Create one with /newlabel</i>"
    lines = ["🏷️ <b>Your Labels</b>\n"]
    for l in labels:
        lines.append(f"  {l['emoji']} <b>{escape(l['name'])}</b>  <i>(id: {l['id']})</i>")
    lines.append("\n<i>Manage: /newlabel, /editlabel, /deletelabel</i>")
    return "\n".join(lines)


def format_label_prompt(task_id: int, description: str | None = None) -> str:
    name = f"\"{escape(description)}\"" if description else f"task #{task_id}"
    return f"🏷️ <b>Pick labels for {name}</b>\n<i>Tap to assign, then tap ✅ Done</i>"


# ── Help & Start ──────────────────────────────────────────────────

def format_start() -> str:
    return (
        "👋 <b>Hey there! I'm your Task & Reminder Bot.</b>\n\n"
        "I help you stay on top of your day. Here's what I can do:\n\n"
        "📝 <b>Add tasks</b> — just type naturally!\n"
        "  <i>\"Buy groceries tomorrow at 3pm\"</i>\n"
        "  <i>\"Clean the house every Saturday\"</i>\n\n"
        "⏰ <b>Reminders</b> — I'll nudge you before deadlines\n"
        "☀️ <b>Morning check-in</b> — I'll ask about your day at 7 AM\n"
        "🌙 <b>Evening review</b> — we'll recap at 9 PM\n"
        "🔄 <b>Recurring tasks</b> — daily, weekly, you name it\n"
        "🏷️ <b>Labels</b> — organize with tags like 🏠 Home, 💼 Work\n\n"
        "Type /help to see all commands!"
    )


def format_help() -> str:
    return (
        "📖 <b>Commands</b>\n\n"
        "<b>Tasks</b>\n"
        "  /add <code>desc | date [time]</code> — Add a task\n"
        "  /tasks — Today's tasks\n"
        "  /upcoming — All upcoming tasks\n"
        "  /done <code>id</code> — Mark task done\n"
        "  /delete <code>id</code> — Delete a task\n"
        "  /edit <code>id field value</code> — Edit a task\n"
        "  /review — Trigger daily review\n"
        "  /stoprecur <code>id</code> — Stop a recurring task\n\n"
        "<b>Labels</b>\n"
        "  /labels — List all labels\n"
        "  /newlabel <code>emoji name</code> — Create a label\n"
        "  /editlabel <code>name emoji newname</code> — Edit a label\n"
        "  /deletelabel <code>name</code> — Delete a label\n"
        "  /filter <code>label</code> — Filter tasks by label\n\n"
        "<b>Routine</b>\n"
        "  /routine — Show morning routine\n"
        "  /routine add <code>desc [at time]</code> — Add item\n"
        "  /routine remove <code>number</code> — Remove item\n\n"
        "<b>More</b>\n"
        "  /undo — Undo last action\n"
        "  /status — Daily overview\n"
        "  /completed <code>[today|week|month|all]</code> — Completed tasks\n"
        "  /history <code>[today|week|month|all]</code> — Full task history\n"
        "  /backup — Download database backup\n"
        "  /clear <code>today|overdue|upcoming|all</code> — Clear tasks\n\n"
        "<b>Or just type naturally!</b>\n"
        "  <i>\"Remind me to call Mom tomorrow at 2pm\"</i>\n"
        "  <i>\"Gym every Monday and Wednesday\"</i>"
    )


# ── Edit ──────────────────────────────────────────────────────────

def format_task_edited(task_id: int, changes: dict, reason: str = "edit",
                       task_description: str | None = None) -> str:
    name = f"\"{escape(task_description)}\"" if task_description else f"Task #{task_id}"
    parts = []
    if "description" in changes:
        parts.append(f"  ✏️ New name → {escape(changes['description'])}")
    if "due_date" in changes:
        parts.append(f"  📅 Date → {_humanize_date(changes['due_date'])}")
    if "due_time" in changes:
        parts.append(f"  ⏰ Time → {changes['due_time']}")

    if reason == "move":
        header = f"📦 <b>{name}</b> moved!"
    elif reason == "rename":
        header = f"📝 <b>{name}</b> renamed!"
    else:
        header = f"✏️ <b>{name}</b> updated!"

    return header + "\n\n" + "\n".join(parts)


# ── Snooze ────────────────────────────────────────────────────────

def format_snoozed(task_id: int, new_date: str, new_time: str | None) -> str:
    time_str = f" at {new_time}" if new_time else ""
    return f"😴 <b>Task #{task_id} snoozed</b> → {_humanize_date(new_date)}{time_str}"


# ── Overdue ───────────────────────────────────────────────────────

def format_overdue_warning(overdue_tasks: list, labels_map: dict | None = None) -> str:
    if not overdue_tasks:
        return ""
    lines = [f"⚠️ <b>Overdue ({len(overdue_tasks)})</b>\n"]
    for t in overdue_tasks:
        task_labels = labels_map.get(t["id"]) if labels_map else None
        lines.append(format_task_line(t, labels=task_labels, show_date=True))
    return "\n".join(lines)


# ── Weekly Summary ────────────────────────────────────────────────

def format_weekly_summary(stats: dict) -> str:
    total = stats["done"] + stats["pending"] + stats["cancelled"]
    lines = [
        "📊 <b>Weekly Summary</b>\n",
        f"  ✅ Completed: <b>{stats['done']}</b>",
        f"  ⏳ Carried over: <b>{stats['pending']}</b>",
        f"  🗑️ Dropped: <b>{stats['cancelled']}</b>",
        f"  📋 Total: <b>{total}</b>",
    ]
    if total > 0:
        pct = round(stats["done"] / total * 100)
        lines.append(f"\n  🎯 Completion rate: <b>{pct}%</b>")
    lines.append("\n<i>Keep up the momentum!</i>")
    return "\n".join(lines)


# ── Undo ──────────────────────────────────────────────────────────

def format_undo_success(action_type: str, task_id: int) -> str:
    labels = {"done": "un-completed", "delete": "restored", "cancel": "un-cancelled", "edit": "reverted"}
    label = labels.get(action_type, "reversed")
    return f"↩️ <b>Undone!</b> Task #{task_id} has been {label}."


def format_undo_expired() -> str:
    return "⏰ <b>Nothing to undo.</b> Undo expires after 5 minutes."


def format_undo_nothing() -> str:
    return "🤷 <b>Nothing to undo.</b> No recent action to reverse."


# ── Status ────────────────────────────────────────────────────────

def format_status(today_count: int, overdue_count: int,
                  upcoming_count: int, completed_today: int) -> str:
    lines = [
        "📊 <b>Your Status</b>\n",
        f"  📋 Today's tasks: <b>{today_count}</b>",
        f"  ✅ Completed today: <b>{completed_today}</b>",
    ]
    if overdue_count > 0:
        lines.append(f"  ⚠️ Overdue: <b>{overdue_count}</b>")
    lines.append(f"  📅 Upcoming (7 days): <b>{upcoming_count}</b>")
    return "\n".join(lines)


# ── History ───────────────────────────────────────────────────────

def format_history(tasks: list, period_label: str, labels_map: dict | None = None) -> str:
    """Format full history showing all task statuses."""
    if not tasks:
        return f"📜 <b>History ({period_label})</b>\n\n<i>No tasks in this period.</i>"

    done = sum(1 for t in tasks if t["status"] == "done")
    cancelled = sum(1 for t in tasks if t["status"] == "cancelled")
    pending = sum(1 for t in tasks if t["status"] == "pending")

    lines = [f"📜 <b>History ({period_label})</b>\n"]
    current_date = None
    for t in tasks:
        if t["due_date"] != current_date:
            current_date = t["due_date"]
            lines.append(f"\n<b>{_humanize_date(current_date).capitalize()}</b>")
        status = t["status"]
        if status == "done":
            icon = "✅"
        elif status == "cancelled":
            icon = "🗑️"
        else:
            icon = "⏳"
        desc = escape(t["description"])
        time_str = f" at {t['due_time']}" if _safe_get(t, "due_time") else ""
        task_labels = labels_map.get(t["id"]) if labels_map else None
        label_str = " " + " ".join(l["emoji"] for l in task_labels) if task_labels else ""
        lines.append(f"{icon} {desc}{time_str}{label_str}")

    summary_parts = []
    if done:
        summary_parts.append(f"✅ {done} done")
    if cancelled:
        summary_parts.append(f"🗑️ {cancelled} dropped")
    if pending:
        summary_parts.append(f"⏳ {pending} pending")
    lines.append(f"\n<i>{' · '.join(summary_parts)} · {len(tasks)} total</i>")
    return "\n".join(lines)


def format_completed(tasks: list, period_label: str, labels_map: dict | None = None) -> str:
    """Format completed tasks list."""
    if not tasks:
        return f"✅ <b>Completed ({period_label})</b>\n\n<i>No tasks completed in this period.</i>"
    lines = [f"✅ <b>Completed ({period_label})</b>\n"]
    current_date = None
    for t in tasks:
        if t["due_date"] != current_date:
            current_date = t["due_date"]
            lines.append(f"\n<b>{_humanize_date(current_date).capitalize()}</b>")
        task_labels = labels_map.get(t["id"]) if labels_map else None
        lines.append(format_task_line(t, labels=task_labels))
    lines.append(f"\n<i>Total: {len(tasks)} task(s)</i>")
    return "\n".join(lines)


# ── Misc ──────────────────────────────────────────────────────────

def format_disambiguate(tasks: list) -> str:
    """Ask the user to pick from multiple matching tasks."""
    lines = ["🤔 <b>Multiple tasks match. Which one?</b>\n"]
    for i, t in enumerate(tasks, 1):
        time_str = f" at {t['due_time']}" if _safe_get(t, "due_time") else ""
        recur = " 🔄" if _safe_get(t, "recurrence_rule") else ""
        lines.append(f"  <b>{i}.</b> {escape(t['description'])}{time_str} — {_humanize_date(t['due_date'])}{recur}")
    lines.append(f"\n<i>Reply with 1–{len(tasks)}</i>")
    return "\n".join(lines)


def format_error(msg: str) -> str:
    return f"❌ {escape(msg)}"


# ── Routine ───────────────────────────────────────────────────────

def format_routine_checklist(items: list, completed_ids: set) -> str:
    if not items:
        return ""
    lines = ["🌅 <b>Morning Routine</b>\n"]
    for i, item in enumerate(items, 1):
        check = "✅" if item["id"] in completed_ids else "⬜"
        time_str = f" ({item['target_time']})" if item["target_time"] else ""
        lines.append(f"  {check} {i}. {escape(item['description'])}{time_str}")
    return "\n".join(lines)


def format_routine_list(items: list) -> str:
    if not items:
        return "🌅 <b>Morning Routine</b>\n\n<i>No routine items yet. Add one with /routine add</i>"
    lines = ["🌅 <b>Morning Routine</b>\n"]
    for i, item in enumerate(items, 1):
        time_str = f" at {item['target_time']}" if item["target_time"] else ""
        lines.append(f"  {i}. {escape(item['description'])}{time_str}")
    lines.append("\n<i>Manage: /routine add, /routine remove</i>")
    return "\n".join(lines)


def format_week_preview(counts: dict[str, int]) -> str:
    if not counts:
        return ""
    lines = ["📆 <b>This Week</b>\n"]
    for date_str in sorted(counts.keys()):
        try:
            d = date.fromisoformat(date_str)
        except ValueError:
            continue
        day_label = _humanize_date(date_str).capitalize()
        lines.append(f"  {day_label}: <b>{counts[date_str]}</b>")
    return "\n".join(lines)


def format_confirm_task(parsed) -> str:
    time_str = f" at {parsed.due_time}" if parsed.due_time else ""
    recur_str = f"\n🔄 Repeats: {_humanize_rule(parsed.recurrence_rule)}" if parsed.recurrence_rule else ""
    label_str = ""
    if parsed.label_names:
        label_str = "\n🏷️ " + ", ".join(escape(n) for n in parsed.label_names)
    return (
        f"🤔 <b>Did you mean:</b>\n\n"
        f"📝 \"{escape(parsed.description)}\"\n"
        f"📅 {_humanize_date(parsed.due_date)}{time_str}"
        f"{recur_str}{label_str}\n\n"
        f"<i>Is this correct?</i>"
    )


def format_not_understood() -> str:
    return (
        "🤷 <b>I didn't quite get that.</b>\n\n"
        "Try again or use a command:\n"
        "  /add <code>Description | YYYY-MM-DD HH:MM</code>\n"
        "  /tasks — today's tasks\n"
        "  /upcoming — all upcoming tasks"
    )
