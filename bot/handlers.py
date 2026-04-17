import functools
import logging
import re
import sqlite3
from datetime import datetime, timedelta
from html import escape

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.config import AUTHORIZED_USER_ID, TIMEZONE, UNDO_EXPIRY_SECONDS

logger = logging.getLogger(__name__)
from bot import database as db
from bot import nlp
from bot import formatting as fmt
from bot.models import ParsedTask
from bot.utils import store_undo, task_to_dict


def authorized(func):
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != AUTHORIZED_USER_ID:
            return
        return await func(update, context)
    return wrapper


def _build_labels_map(tasks: list) -> dict:
    """Build {task_id: [label_rows]} for a list of tasks in a single query."""
    if not tasks:
        return {}
    return db.get_labels_for_tasks([t["id"] for t in tasks])


def _store_pos_map(context, pos_map: dict):
    """Store position→task_id mapping so users can reference tasks by list number."""
    if pos_map:
        context.application.bot_data["task_pos_map"] = pos_map


def _remaining_today_summary(context=None) -> str:
    """Return a short summary of remaining tasks for today, or empty string."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    remaining = db.get_tasks_for_date(today)
    if not remaining:
        return "\n\n🎉 <i>All done for today!</i>"
    labels_map = _build_labels_map(remaining)
    text, pos_map = fmt.format_task_list(
        f"📋 <b>Remaining for today ({len(remaining)})</b>", remaining, labels_map,
    )
    if context:
        _store_pos_map(context, pos_map)
    return "\n\n" + text


def _build_label_keyboard(task_id: int, selected: set | None = None) -> InlineKeyboardMarkup:
    """Build inline keyboard with all labels for selection."""
    labels = db.get_all_labels()
    selected = selected or set()
    buttons = []
    row = []
    for l in labels:
        check = "✓ " if l["id"] in selected else ""
        row.append(InlineKeyboardButton(
            f"{check}{l['emoji']} {l['name']}",
            callback_data=f"label_toggle_{task_id}_{l['id']}",
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([
        InlineKeyboardButton("✅ Done", callback_data=f"label_done_{task_id}"),
        InlineKeyboardButton("⏩ Skip", callback_data=f"label_skip_{task_id}"),
    ])
    return InlineKeyboardMarkup(buttons)


async def _assign_labels_from_names(task_id: int, label_names: list[str]) -> list:
    """Resolve label names to IDs and assign to task. Returns assigned label rows."""
    assigned = []
    for name in label_names:
        label = db.get_label_by_name(name)
        if label:
            db.add_task_label(task_id, label["id"])
            assigned.append(label)
    return assigned


def _parse_user_time(text: str) -> str | None:
    """Parse a casual time string like '10pm', '3:30pm', '23:00', '9 am' into HH:MM format."""
    import re
    text = text.strip().lower().replace(" ", "")
    # Match patterns: 10pm, 3:30pm, 23:00, 9am, 10:00
    m = re.match(r'^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$', text)
    if not m:
        return None
    hour = int(m.group(1))
    minute = int(m.group(2)) if m.group(2) else 0
    period = m.group(3)
    if period == "pm" and hour < 12:
        hour += 12
    elif period == "am" and hour == 12:
        hour = 0
    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def _parse_natural_date(text: str) -> tuple[str, str | None] | None:
    """Parse natural language date strings like 'tomorrow', 'next Monday', 'Friday'.
    Returns (YYYY-MM-DD, HH:MM or None) or None if not recognized."""
    import re
    text = text.strip().lower()
    now = datetime.now(TIMEZONE)
    today = now.date()

    DAY_MAP = {
        "monday": 0, "mon": 0, "tuesday": 1, "tue": 1, "tues": 1,
        "wednesday": 2, "wed": 2, "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
        "friday": 4, "fri": 4, "saturday": 5, "sat": 5, "sunday": 6, "sun": 6,
    }

    # Extract optional time suffix: "tomorrow at 3pm", "friday 10:00"
    time_part = None
    time_match = re.search(r'(?:at\s+)?(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*$', text)
    if time_match:
        parsed_time = _parse_user_time(time_match.group(1))
        if parsed_time:
            time_part = parsed_time
            text = text[:time_match.start()].strip()

    if text in ("today",):
        return today.isoformat(), time_part
    if text in ("tomorrow", "tmr", "tmrw"):
        return (today + timedelta(days=1)).isoformat(), time_part
    if text in ("day after tomorrow",):
        return (today + timedelta(days=2)).isoformat(), time_part

    # "in N days" / "in a week"
    in_match = re.match(r'^in\s+(\d+)\s+days?$', text)
    if in_match:
        n = int(in_match.group(1))
        return (today + timedelta(days=n)).isoformat(), time_part
    if text == "in a week":
        return (today + timedelta(days=7)).isoformat(), time_part

    # "this weekend" = next Saturday
    if text in ("this weekend", "weekend"):
        days_ahead = (5 - today.weekday()) % 7 or 7
        return (today + timedelta(days=days_ahead)).isoformat(), time_part

    # "next week" = next Monday
    if text == "next week":
        days_ahead = (0 - today.weekday()) % 7 or 7
        return (today + timedelta(days=days_ahead)).isoformat(), time_part

    # "next <day>" or just "<day>"
    next_prefix = False
    if text.startswith("next "):
        next_prefix = True
        text = text[5:].strip()

    if text in DAY_MAP:
        target_weekday = DAY_MAP[text]
        days_ahead = (target_weekday - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7  # same day = next week
        if next_prefix and days_ahead < 7:
            days_ahead += 7  # "next Friday" when it's already ahead this week
        return (today + timedelta(days=days_ahead)).isoformat(), time_part

    return None


def _is_past_time(due_date: str, due_time: str | None) -> bool:
    """Check if a task's date+time is already in the past."""
    if not due_time:
        return False
    now = datetime.now(TIMEZONE)
    try:
        task_dt = datetime.strptime(f"{due_date} {due_time}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
        return task_dt < now
    except ValueError:
        return False


async def _add_task_and_respond(update, context, parsed):
    """Add a task from ParsedTask and send styled response + label prompt."""
    # Check if time is already past
    if _is_past_time(parsed.due_date, parsed.due_time):
        context.user_data["pending_past_task"] = parsed
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📅 Move to tomorrow", callback_data="past_task_tomorrow"),
            InlineKeyboardButton("❌ Cancel", callback_data="past_task_cancel"),
        ]])
        await update.message.reply_text(
            f"⏰ <b>Heads up!</b> {parsed.due_time} today has already passed.\n\n"
            f"📝 \"{escape(parsed.description)}\"\n\n"
            f"Reply with a new time (e.g. <b>10pm</b>, <b>23:00</b>) or tap a button.",
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return None

    task_id = db.add_task(
        parsed.description, parsed.due_date, parsed.due_time,
        recurrence_rule=parsed.recurrence_rule,
        notes=parsed.notes,
    )

    # Auto-assign labels from NLP
    labels = await _assign_labels_from_names(task_id, parsed.label_names)

    msg = fmt.format_task_added(
        task_id, parsed.description, parsed.due_date, parsed.due_time,
        parsed.recurrence_rule, labels, notes=parsed.notes,
    )
    await update.message.reply_text(msg, parse_mode="HTML")

    # Prompt for labels if none were auto-assigned
    if not labels:
        keyboard = _build_label_keyboard(task_id)
        await update.message.reply_text(
            fmt.format_label_prompt(task_id, parsed.description),
            parse_mode="HTML",
            reply_markup=keyboard,
        )

    return task_id


# ── Commands ──────────────────────────────────────────────────────

@authorized
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(fmt.format_start(), parse_mode="HTML")


@authorized
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(fmt.format_help(), parse_mode="HTML")


@authorized
async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.removeprefix("/add").strip()
    if "|" not in text:
        await update.message.reply_text(
            fmt.format_error("Format: /add Description | YYYY-MM-DD HH:MM"),
            parse_mode="HTML",
        )
        return

    parts = text.split("|", 1)
    description = parts[0].strip()
    datetime_str = parts[1].strip()

    tokens = datetime_str.split()
    due_date = tokens[0]
    due_time = tokens[1] if len(tokens) > 1 else None

    try:
        datetime.strptime(due_date, "%Y-%m-%d")
        if due_time:
            datetime.strptime(due_time, "%H:%M")
    except ValueError:
        await update.message.reply_text(
            fmt.format_error("Invalid date/time format. Use YYYY-MM-DD HH:MM"),
            parse_mode="HTML",
        )
        return

    task_id = db.add_task(description, due_date, due_time)
    msg = fmt.format_task_added(task_id, description, due_date, due_time, None, None)
    await update.message.reply_text(msg, parse_mode="HTML")

    # Prompt for labels
    keyboard = _build_label_keyboard(task_id)
    await update.message.reply_text(
        fmt.format_label_prompt(task_id, description), parse_mode="HTML", reply_markup=keyboard,
    )


@authorized
async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    tasks = db.get_tasks_for_date(today)
    overdue = db.get_overdue_tasks()
    labels_map = _build_labels_map(tasks)

    header = "📋 <b>Today's Tasks</b>"
    if overdue:
        header += f"  ⚠️ <i>{len(overdue)} overdue</i>"
    msg, pos_map = fmt.format_task_list(header, tasks, labels_map)
    _store_pos_map(context, pos_map)

    if overdue:
        overdue_labels = _build_labels_map(overdue)
        msg += "\n\n" + fmt.format_overdue_warning(overdue, overdue_labels)

    await update.message.reply_text(msg, parse_mode="HTML")


@authorized
async def upcoming_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = db.get_upcoming_tasks()
    labels_map = _build_labels_map(tasks)
    msg, pos_map = fmt.format_task_list("📅 <b>Upcoming Tasks</b>", tasks, labels_map, show_date=True)
    _store_pos_map(context, pos_map)
    await update.message.reply_text(msg, parse_mode="HTML")


@authorized
async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(fmt.format_error("Usage: /done <task_id>"), parse_mode="HTML")
        return
    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text(fmt.format_error("Task ID must be a number."), parse_mode="HTML")
        return

    task = db.get_task(task_id)
    if not task:
        await update.message.reply_text(fmt.format_error(f"Task #{task_id} not found."), parse_mode="HTML")
        return

    task_date = task["due_date"]
    store_undo(context, "done", task_id, task_to_dict(task))
    db.update_task_status(task_id, "done")

    # Handle recurrence
    next_id = None
    if task["recurrence_rule"] and task["recurrence_active"]:
        next_id = db.create_next_occurrence(task_id)

    msg = fmt.format_task_done(task, next_id)
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    if task_date == today:
        msg += _remaining_today_summary(context)
    await update.message.reply_text(msg, parse_mode="HTML")


@authorized
async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(fmt.format_error("Usage: /delete <task_id>"), parse_mode="HTML")
        return
    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text(fmt.format_error("Task ID must be a number."), parse_mode="HTML")
        return

    task = db.get_task(task_id)
    if not task:
        await update.message.reply_text(fmt.format_error(f"Task #{task_id} not found."), parse_mode="HTML")
        return

    task_date = task["due_date"]
    store_undo(context, "delete", task_id, task_to_dict(task))
    db.delete_task(task_id)
    msg = f"🗑️ <b>Deleted:</b> \"{escape(task['description'])}\""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    if task_date == today:
        msg += _remaining_today_summary(context)
    await update.message.reply_text(msg, parse_mode="HTML")


@authorized
async def review_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from bot.callbacks import send_daily_review
    await send_daily_review(context)


@authorized
async def stop_recurring_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(fmt.format_error("Usage: /stoprecur <task_id>"), parse_mode="HTML")
        return
    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text(fmt.format_error("Task ID must be a number."), parse_mode="HTML")
        return

    task = db.get_task(task_id)
    if not task or not task["recurrence_rule"]:
        await update.message.reply_text(
            fmt.format_error("That's not a recurring task."), parse_mode="HTML",
        )
        return

    db.stop_recurrence(task_id)
    await update.message.reply_text(
        f"🛑 Recurrence stopped for task #{task_id}.", parse_mode="HTML",
    )


# ── Label commands ────────────────────────────────────────────────

@authorized
async def labels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    labels = db.get_all_labels()
    await update.message.reply_text(fmt.format_labels_list(labels), parse_mode="HTML")


@authorized
async def newlabel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.removeprefix("/newlabel").strip()
    if len(text) < 2:
        await update.message.reply_text(
            fmt.format_error("Usage: /newlabel 🎵 Music"), parse_mode="HTML",
        )
        return

    # First character(s) before space = emoji, rest = name
    parts = text.split(None, 1)
    if len(parts) < 2:
        await update.message.reply_text(
            fmt.format_error("Usage: /newlabel 🎵 Music"), parse_mode="HTML",
        )
        return

    emoji, name = parts[0], parts[1].strip()
    try:
        label_id = db.add_label(emoji, name)
        await update.message.reply_text(
            f"🏷️ Label created: {escape(emoji)} <b>{escape(name)}</b> (id: {label_id})",
            parse_mode="HTML",
        )
    except sqlite3.IntegrityError:
        await update.message.reply_text(
            fmt.format_error(f"Label '{name}' already exists."), parse_mode="HTML",
        )


@authorized
async def editlabel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.removeprefix("/editlabel").strip()
    parts = text.split(None, 2)
    if len(parts) < 3:
        await update.message.reply_text(
            fmt.format_error("Usage: /editlabel OldName 🎶 NewName"), parse_mode="HTML",
        )
        return

    old_name, new_emoji, new_name = parts[0], parts[1], parts[2]
    label = db.get_label_by_name(old_name)
    if not label:
        await update.message.reply_text(
            fmt.format_error(f"Label '{old_name}' not found."), parse_mode="HTML",
        )
        return

    db.update_label(label["id"], emoji=new_emoji, name=new_name)
    await update.message.reply_text(
        f"🏷️ Label updated: {new_emoji} <b>{new_name}</b>", parse_mode="HTML",
    )


@authorized
async def deletelabel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.removeprefix("/deletelabel").strip()
    if not name:
        await update.message.reply_text(
            fmt.format_error("Usage: /deletelabel Music"), parse_mode="HTML",
        )
        return

    label = db.get_label_by_name(name)
    if not label:
        await update.message.reply_text(
            fmt.format_error(f"Label '{name}' not found."), parse_mode="HTML",
        )
        return

    db.delete_label(label["id"])
    await update.message.reply_text(
        f"🗑️ Label <b>{name}</b> deleted.", parse_mode="HTML",
    )


@authorized
async def filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.removeprefix("/filter").strip()
    if not name:
        await update.message.reply_text(
            fmt.format_error("Usage: /filter Work"), parse_mode="HTML",
        )
        return

    label = db.get_label_by_name(name)
    if not label:
        await update.message.reply_text(
            fmt.format_error(f"Label '{name}' not found."), parse_mode="HTML",
        )
        return

    tasks = db.get_tasks_by_label(label["id"])
    labels_map = _build_labels_map(tasks)
    title = f"{label['emoji']} <b>{label['name']} Tasks</b>"
    msg, pos_map = fmt.format_task_list(title, tasks, labels_map, show_date=True)
    _store_pos_map(context, pos_map)
    await update.message.reply_text(
        msg,
        parse_mode="HTML",
    )


# ── Edit, Undo, Status, History, Backup commands ─────────────────

@authorized
async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or len(args) < 3:
        await update.message.reply_text(
            fmt.format_error("Usage: /edit <id> <field> <value>\nFields: date, time, desc"),
            parse_mode="HTML",
        )
        return

    try:
        task_id = int(args[0])
    except ValueError:
        await update.message.reply_text(fmt.format_error("Task ID must be a number."), parse_mode="HTML")
        return

    task = db.get_task(task_id)
    if not task:
        await update.message.reply_text(fmt.format_error(f"Task #{task_id} not found."), parse_mode="HTML")
        return

    field = args[1].lower()
    value = " ".join(args[2:])
    changes = {}

    if field == "date":
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            await update.message.reply_text(fmt.format_error("Invalid date. Use YYYY-MM-DD"), parse_mode="HTML")
            return
        changes["due_date"] = value
    elif field == "time":
        try:
            datetime.strptime(value, "%H:%M")
        except ValueError:
            await update.message.reply_text(fmt.format_error("Invalid time. Use HH:MM"), parse_mode="HTML")
            return
        changes["due_time"] = value
    elif field in ("desc", "description"):
        changes["description"] = value
    else:
        await update.message.reply_text(fmt.format_error("Field must be: date, time, or desc"), parse_mode="HTML")
        return

    store_undo(context, "edit", task_id, task_to_dict(task))
    db.update_task(task_id,
                   description=changes.get("description"),
                   due_date=changes.get("due_date"),
                   due_time=changes.get("due_time"))
    await update.message.reply_text(
        fmt.format_task_edited(task_id, changes, task_description=task["description"]),
        parse_mode="HTML",
    )


@authorized
async def undo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import time as time_mod
    undo_data = context.application.bot_data.get("last_undo")
    if not undo_data:
        await update.message.reply_text(fmt.format_undo_nothing(), parse_mode="HTML")
        return

    elapsed = time_mod.time() - undo_data.get("timestamp", 0)
    if elapsed > UNDO_EXPIRY_SECONDS:
        context.application.bot_data.pop("last_undo", None)
        await update.message.reply_text(fmt.format_undo_expired(), parse_mode="HTML")
        return

    action_type = undo_data["type"]
    task_id = undo_data["task_id"]
    prev = undo_data["previous_state"]

    if action_type == "done":
        db.update_task_status(task_id, "pending")
    elif action_type == "delete":
        db.reinsert_task(prev)
    elif action_type == "cancel":
        db.update_task_status(task_id, "pending")
    elif action_type == "edit":
        db.update_task(task_id,
                       description=prev.get("description"),
                       due_date=prev.get("due_date"),
                       due_time=prev.get("due_time"))

    context.application.bot_data.pop("last_undo", None)
    await update.message.reply_text(fmt.format_undo_success(action_type, task_id), parse_mode="HTML")


@authorized
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    today_tasks = db.get_tasks_for_date(today)
    overdue = db.get_overdue_tasks()
    upcoming = db.get_upcoming_tasks()
    completed_today = db.get_completed_tasks_for_date(today)

    msg = fmt.format_status(
        today_count=len(today_tasks),
        overdue_count=len(overdue),
        upcoming_count=len(upcoming),
        completed_today=len(completed_today),
    )
    await update.message.reply_text(msg, parse_mode="HTML")


def _resolve_period(period: str):
    """Resolve a period string to (start_date, end_date, label) or None if invalid."""
    today = datetime.now(TIMEZONE)
    today_str = today.strftime("%Y-%m-%d")
    if period == "today":
        return today_str, today_str, "Today"
    elif period == "week":
        return (today - timedelta(days=7)).strftime("%Y-%m-%d"), today_str, "Past 7 Days"
    elif period == "month":
        return (today - timedelta(days=30)).strftime("%Y-%m-%d"), today_str, "Past 30 Days"
    elif period == "all":
        return None, None, "All Time"
    return None


async def _show_history(update, period: str):
    """Show full task history (all statuses)."""
    result = _resolve_period(period)
    if not result:
        await update.message.reply_text(
            fmt.format_error("Usage: /history [today|week|month|all]"), parse_mode="HTML",
        )
        return
    start, end, label = result
    tasks = db.get_all_tasks_in_range(start, end)
    labels_map = _build_labels_map(tasks)
    await update.message.reply_text(fmt.format_history(tasks, label, labels_map), parse_mode="HTML")


async def _show_completed(update, period: str):
    """Show only completed tasks."""
    result = _resolve_period(period)
    if not result:
        await update.message.reply_text(
            fmt.format_error("Usage: /completed [today|week|month|all]"), parse_mode="HTML",
        )
        return
    start, end, label = result
    tasks = db.get_completed_tasks(start, end)
    labels_map = _build_labels_map(tasks)
    await update.message.reply_text(fmt.format_completed(tasks, label, labels_map), parse_mode="HTML")


@authorized
async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    period = args[0].lower() if args else "week"
    await _show_history(update, period)


@authorized
async def completed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    period = args[0].lower() if args else "today"
    await _show_completed(update, period)


@authorized
async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import sqlite3
    import tempfile
    from bot.config import DB_PATH

    import os
    import sys
    filename = f"tasks_backup_{datetime.now(TIMEZONE).strftime('%Y-%m-%d')}.db"
    tmp_path = None
    try:
        # Use sqlite3 backup API for a consistent snapshot
        src = sqlite3.connect(str(DB_PATH))
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp_path = tmp.name
        tmp.close()
        dst = sqlite3.connect(tmp_path)
        src.backup(dst)
        src.close()
        dst.close()
        if sys.platform != "win32":
            os.chmod(tmp_path, 0o600)

        with open(tmp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename=filename,
            )
    except Exception:
        logger.exception("Backup failed")
        await update.message.reply_text(fmt.format_error("Backup failed. Check logs."), parse_mode="HTML")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ── Routine command ───────────────────────────────────────────────

@authorized
async def routine_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.removeprefix("/routine").strip()

    if not text or text.lower() in ("list", "show"):
        items = db.get_all_routine_items()
        await update.message.reply_text(fmt.format_routine_list(items), parse_mode="HTML")
        return

    if text.lower().startswith("add"):
        desc_text = text[3:].strip()
        if not desc_text:
            await update.message.reply_text(
                fmt.format_error("Usage: /routine add Drink water at 7am"), parse_mode="HTML",
            )
            return
        # Parse optional "at TIME" from the end
        target_time = None
        import re
        m = re.search(r'\bat\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\s*$', desc_text, re.IGNORECASE)
        if m:
            target_time = _parse_user_time(m.group(1))
            if target_time:
                desc_text = desc_text[:m.start()].strip()
        if not desc_text:
            await update.message.reply_text(
                fmt.format_error("Please provide a description."), parse_mode="HTML",
            )
            return
        item_id = db.add_routine_item(desc_text, target_time)
        time_str = f" at {target_time}" if target_time else ""
        await update.message.reply_text(
            f"🌅 <b>Added to routine:</b> {escape(desc_text)}{time_str}", parse_mode="HTML",
        )
        return

    if text.lower().startswith("remove") or text.lower().startswith("delete"):
        arg = text.split(None, 1)[1].strip() if " " in text else ""
        if not arg:
            await update.message.reply_text(
                fmt.format_error("Usage: /routine remove <number or name>"), parse_mode="HTML",
            )
            return
        # Try as number (position in list)
        items = db.get_all_routine_items()
        try:
            pos = int(arg)
            if 1 <= pos <= len(items):
                item = items[pos - 1]
                db.delete_routine_item(item["id"])
                await update.message.reply_text(
                    f"🗑️ Removed from routine: <b>{escape(item['description'])}</b>", parse_mode="HTML",
                )
                return
        except ValueError:
            pass
        # Try as description match
        item = db.get_routine_item_by_description(arg)
        if item:
            db.delete_routine_item(item["id"])
            await update.message.reply_text(
                f"🗑️ Removed from routine: <b>{escape(item['description'])}</b>", parse_mode="HTML",
            )
        else:
            await update.message.reply_text(
                fmt.format_error(f"No routine item matching \"{arg}\"."), parse_mode="HTML",
            )
        return

    await update.message.reply_text(
        fmt.format_error("Usage: /routine [list|add|remove]"), parse_mode="HTML",
    )


VALID_CLEAR_SCOPES = {"today", "overdue", "upcoming", "all_tasks", "all_labels", "everything"}

CLEAR_LABELS = {
    "today": "today's pending tasks",
    "overdue": "overdue pending tasks",
    "upcoming": "all upcoming tasks",
    "all_tasks": "ALL tasks (including completed)",
    "all_labels": "ALL labels",
    "everything": "ALL tasks AND labels",
}


def _build_clear_ask_keyboard() -> InlineKeyboardMarkup:
    """Build keyboard asking what to clear."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Today's tasks", callback_data="clear_confirm_today"),
            InlineKeyboardButton("⚠️ Overdue tasks", callback_data="clear_confirm_overdue"),
        ],
        [
            InlineKeyboardButton("📅 Upcoming tasks", callback_data="clear_confirm_upcoming"),
            InlineKeyboardButton("🗑️ All tasks", callback_data="clear_confirm_all_tasks"),
        ],
        [
            InlineKeyboardButton("🏷️ All labels", callback_data="clear_confirm_all_labels"),
            InlineKeyboardButton("💣 Everything", callback_data="clear_confirm_everything"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="clear_cancel")],
    ])


async def _send_clear_confirmation(message, scope: str, excluded_ids: set | None = None):
    """Send the appropriate clear confirmation for a given scope."""
    if scope == "ask" or scope not in CLEAR_LABELS:
        await message.reply_text(
            "🧹 <b>What do you want to clear?</b>",
            parse_mode="HTML",
            reply_markup=_build_clear_ask_keyboard(),
        )
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Yes, clear them", callback_data=f"clear_confirm_{scope}"),
        InlineKeyboardButton("Cancel", callback_data="clear_cancel"),
    ]])
    skip_note = ""
    if excluded_ids:
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        if scope == "overdue":
            tasks = db.get_overdue_tasks()
        else:
            tasks = db.get_tasks_for_date(today)
        skip_names = ", ".join(
            escape(t["description"]) for t in tasks if t["id"] in excluded_ids
        )
        if skip_names:
            skip_note = f"\n\n⏭️ <b>Keeping:</b> {skip_names}"
    await message.reply_text(
        f"⚠️ <b>Are you sure?</b>\n\nThis will permanently delete <b>{CLEAR_LABELS[scope]}</b>.{skip_note}",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


@authorized
async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    scope = args[0].lower() if args else "ask"
    # Map legacy "all" from /clear command to "all_tasks"
    if scope == "all":
        scope = "all_tasks"
    await _send_clear_confirmation(update.message, scope)


# ── Natural language handler ─────────────────────────────────────

@authorized
async def handle_natural_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Clear stale confirmation states from previous interactions
    context.user_data.pop("pending_bulk_done", None)

    # Check if user is picking from a disambiguation prompt
    pending = context.user_data.get("pending_disambiguation")
    if pending:
        text = (update.message.text or "").strip()
        # Match: "1", "#1", "task 1", "#29" (DB id), "29"
        m = re.match(r"^#?(?:task\s*)?(\d+)$", text, re.IGNORECASE)
        if m:
            pick = int(m.group(1))
            tasks = pending["tasks"]
            # First try as a sequential pick (1-based index shown in prompt)
            if 1 <= pick <= len(tasks):
                chosen = tasks[pick - 1]
            else:
                # Fall back to matching by DB id
                chosen = next((t for t in tasks if t["id"] == pick), None)
            if chosen:
                context.user_data.pop("pending_disambiguation")
                data = pending["data"].copy()
                data["task_id"] = chosen["id"]
                data.pop("task_description", None)
                return await _route_intent(update, context, data, pending["intent"])
            else:
                await update.message.reply_text(
                    fmt.format_error(f"Invalid pick. Reply with 1–{len(tasks)}."),
                    parse_mode="HTML",
                )
                return
        # Not a number — clear disambiguation and fall through to normal NLP
        context.user_data.pop("pending_disambiguation", None)

    # Check if user is replying with a new time for a past-time task
    if context.user_data.get("pending_past_task"):
        parsed = context.user_data.get("pending_past_task")
        text = (update.message.text or "").strip()
        # Try to parse as a time (e.g. "10pm", "23:00", "3:30pm")
        new_time = _parse_user_time(text)
        if new_time:
            parsed.due_time = new_time
            # Check if the new time is also in the past
            if _is_past_time(parsed.due_date, new_time):
                await update.message.reply_text(
                    f"⏰ <b>{new_time}</b> has also passed. Try a later time or tap a button.",
                    parse_mode="HTML",
                )
                return
            context.user_data.pop("pending_past_task", None)
            await _add_task_and_respond(update, context, parsed)
            return
        # If not a time, fall through to normal NLP (don't consume the message)
        context.user_data.pop("pending_past_task", None)

    # Check if we're awaiting a carryover date
    if context.user_data.get("awaiting_carry_date"):
        task_id = context.user_data.pop("awaiting_carry_date")
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text(
                fmt.format_error("Please send a date (e.g. \"tomorrow\", \"Friday\", or YYYY-MM-DD)"),
                parse_mode="HTML",
            )
            context.user_data["awaiting_carry_date"] = task_id
            return

        # Try natural language first ("tomorrow", "next Monday", "Friday")
        natural = _parse_natural_date(text)
        if natural:
            new_date, new_time = natural
            # Preserve existing time if user didn't specify one
            if new_time is None:
                existing = db.get_task(task_id)
                if existing:
                    new_time = existing["due_time"]
            db.carry_over_task(task_id, new_date, new_time)
            await update.message.reply_text(
                fmt.format_review_carried(task_id, new_date), parse_mode="HTML",
            )
            return

        # Fall back to YYYY-MM-DD [HH:MM] format
        tokens = text.split()
        try:
            new_date = tokens[0]
            datetime.strptime(new_date, "%Y-%m-%d")
            new_time = tokens[1] if len(tokens) > 1 else None
            if new_time:
                datetime.strptime(new_time, "%H:%M")
            # Preserve existing time if user didn't specify one
            if new_time is None:
                existing = db.get_task(task_id)
                if existing:
                    new_time = existing["due_time"]
            db.carry_over_task(task_id, new_date, new_time)
            await update.message.reply_text(
                fmt.format_review_carried(task_id, new_date), parse_mode="HTML",
            )
        except (ValueError, IndexError):
            await update.message.reply_text(
                fmt.format_error("Couldn't understand that date. Try \"tomorrow\", \"Friday\", or YYYY-MM-DD."),
                parse_mode="HTML",
            )
            context.user_data["awaiting_carry_date"] = task_id
        return

    # Morning prompt mode — parse multiple tasks
    if context.application.bot_data.get("morning_prompt_active"):
        tasks = await nlp.parse_morning_tasks(update.message.text)
        if not tasks:
            # User said "done" or unparseable
            await _end_morning_session(update, context)
            return

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        added_ids = []
        for parsed in tasks:
            task_id = db.add_task(
                parsed.description, today, parsed.due_time,
                recurrence_rule=parsed.recurrence_rule,
                notes=parsed.notes,
            )
            await _assign_labels_from_names(task_id, parsed.label_names)
            added_ids.append(task_id)
            context.application.bot_data.setdefault("morning_prompt_tasks", []).append(task_id)

        time_strs = []
        for parsed in tasks:
            t = f" at {parsed.due_time}" if parsed.due_time else ""
            time_strs.append(f"  ✅ {escape(parsed.description)}{t}")

        await update.message.reply_text(
            f"📝 <b>Added {len(tasks)} task(s):</b>\n" + "\n".join(time_strs) +
            "\n\n<i>Send more tasks or tap \"I'm done\" when finished.</i>",
            parse_mode="HTML",
        )
        return

    # Regular NLP parsing — pass current labels so LLM knows what's available
    all_labels = db.get_all_labels()
    label_names = [l["name"] for l in all_labels]
    result = await nlp.parse_task_message(update.message.text, available_labels=label_names)

    if result is None:
        await update.message.reply_text(fmt.format_not_understood(), parse_mode="HTML")
        return

    # Handle dict-based intents (everything except add_task)
    if isinstance(result, dict):
        intent = result.get("intent", "unknown")
        return await _route_intent(update, context, result, intent)

    # Handle new task (ParsedTask)
    parsed = result

    if parsed.confidence < 0.7:
        context.user_data["pending_task"] = parsed
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Yes, add it", callback_data="confirm_add"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_add"),
        ]])
        await update.message.reply_text(
            fmt.format_confirm_task(parsed), parse_mode="HTML", reply_markup=keyboard,
        )
    else:
        await _add_task_and_respond(update, context, parsed)


def _resolve_task(data: dict, context=None):
    """Resolve a task from task_id or task_description.
    Returns (task, error_msg, ambiguous_list).
    - Single match: (task, None, None)
    - No match/error: (None, error_msg, None)
    - Multiple matches: (None, None, [tasks])
    """
    task_id = data.get("task_id")
    task_desc = data.get("task_description")

    if task_id:
        # Handle "last" as a special reference to the last item in the list
        if str(task_id).lower() == "last":
            if context:
                pos_map = context.application.bot_data.get("task_pos_map", {})
                if pos_map:
                    last_pos = max(pos_map.keys())
                    tid = pos_map[last_pos]
                    task = db.get_task(tid)
                    if task:
                        return task, None, None
            return None, "No task list available. Use /upcoming first, then try again.", None

        tid = int(task_id)
        # Check if the number refers to a list position
        if context:
            pos_map = context.application.bot_data.get("task_pos_map", {})
            if tid in pos_map:
                tid = pos_map[tid]
        task = db.get_task(tid)
        if not task:
            return None, f"Task #{task_id} not found.", None
        return task, None, None

    if task_desc:
        matches = db.find_tasks_by_description(str(task_desc))
        if not matches:
            return None, f"No pending task matching \"{escape(str(task_desc))}\".", None
        if len(matches) == 1:
            return matches[0], None, None
        return None, None, matches

    return None, "Which task? Give me a task number or name.", None


async def _resolve_or_ask(update, data: dict, context=None, intent: str = None):
    """Resolve task or send disambiguation/error. Returns task or None."""
    task, err, ambiguous = _resolve_task(data, context)
    if err:
        await update.message.reply_text(fmt.format_error(err), parse_mode="HTML")
        return None
    if ambiguous:
        if context:
            context.user_data["pending_disambiguation"] = {
                "tasks": ambiguous,
                "intent": intent or data.get("intent", "unknown"),
                "data": data,
            }
        await update.message.reply_text(fmt.format_disambiguate(ambiguous), parse_mode="HTML")
        return None
    return task


async def _route_intent(update, context, data: dict, intent: str):
    """Route all non-add_task intents from NLP."""

    if intent == "query":
        query_type = data.get("query_type", "today")
        if query_type == "upcoming":
            return await upcoming_command(update, context)
        elif query_type == "review":
            return await review_command(update, context)
        elif query_type == "filter":
            label_name = data.get("filter_label", "").strip()
            if not label_name:
                await update.message.reply_text(fmt.format_error("Which label to filter by?"), parse_mode="HTML")
                return
            label = db.get_label_by_name(label_name)
            if not label:
                await update.message.reply_text(fmt.format_error(f"Label '{label_name}' not found."), parse_mode="HTML")
                return
            tasks = db.get_tasks_by_label(label["id"])
            labels_map = _build_labels_map(tasks)
            title = f"{label['emoji']} <b>{label['name']} Tasks</b>"
            msg, pos_map = fmt.format_task_list(title, tasks, labels_map, show_date=True)
            _store_pos_map(context, pos_map)
            await update.message.reply_text(msg, parse_mode="HTML")
            return
        elif query_type == "overdue":
            overdue = db.get_overdue_tasks()
            if not overdue:
                await update.message.reply_text("✅ <b>No overdue tasks!</b> You're all caught up.", parse_mode="HTML")
                return
            overdue_labels = _build_labels_map(overdue)
            await update.message.reply_text(
                fmt.format_overdue_warning(overdue, overdue_labels), parse_mode="HTML",
            )
            return
        elif query_type == "status":
            return await status_command(update, context)
        elif query_type == "history":
            period = data.get("history_period", "week")
            return await _show_history(update, period)
        elif query_type == "completed":
            period = data.get("history_period", "today")
            return await _show_completed(update, period)
        elif query_type == "date":
            query_date = data.get("query_date")
            if not query_date:
                return await tasks_command(update, context)
            try:
                datetime.strptime(query_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                return await tasks_command(update, context)
            tasks = db.get_tasks_for_date(query_date)
            labels_map = _build_labels_map(tasks)
            human_date = fmt._humanize_date(query_date).capitalize()
            msg, pos_map = fmt.format_task_list(
                f"📋 <b>Tasks for {human_date}</b>", tasks, labels_map,
            )
            _store_pos_map(context, pos_map)
            await update.message.reply_text(msg, parse_mode="HTML")
            return
        else:
            return await tasks_command(update, context)

    elif intent == "done":
        # If no task reference given, try last reminded task as fallback
        if not data.get("task_id") and not data.get("task_description"):
            last_reminded = context.application.bot_data.get("last_reminded_task_id")
            if last_reminded:
                fallback_task = db.get_task(last_reminded)
                if fallback_task and fallback_task["status"] == "pending":
                    data["task_id"] = last_reminded

        task, err, ambiguous = _resolve_task(data, context)
        if err:
            # If no pending match by description, check if it's already done
            task_desc = data.get("task_description")
            if task_desc:
                done_matches = db.find_done_tasks_by_description(str(task_desc))
                if done_matches:
                    match = done_matches[0]
                    tid = match["id"]
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("↩️ Yes, undo it", callback_data=f"undo_done_{tid}"),
                        InlineKeyboardButton("❌ No", callback_data=f"undo_done_cancel"),
                    ]])
                    await update.message.reply_text(
                        f"🔍 <b>\"{escape(match['description'])}\"</b> is already marked as done.\n\n"
                        f"Would you like to undo that and mark it as pending?",
                        parse_mode="HTML",
                        reply_markup=keyboard,
                    )
                    return
            await update.message.reply_text(fmt.format_error(err), parse_mode="HTML")
            return
        if ambiguous:
            await update.message.reply_text(fmt.format_disambiguate(ambiguous), parse_mode="HTML")
            return
        task_date = task["due_date"]
        store_undo(context, "done", task["id"], task_to_dict(task))
        db.update_task_status(task["id"], "done")
        next_id = None
        if task["recurrence_rule"] and task["recurrence_active"]:
            next_id = db.create_next_occurrence(task["id"])
        msg = fmt.format_task_done(task, next_id)
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        if task_date == today:
            msg += _remaining_today_summary(context)
        await update.message.reply_text(msg, parse_mode="HTML")

    elif intent == "delete":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        task_date = task["due_date"]
        store_undo(context, "delete", task["id"], task_to_dict(task))
        db.delete_task(task["id"])
        msg = f"🗑️ <b>Deleted:</b> \"{escape(task['description'])}\""
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        if task_date == today:
            msg += _remaining_today_summary(context)
        await update.message.reply_text(msg, parse_mode="HTML")

    elif intent == "list_labels":
        return await labels_command(update, context)

    elif intent == "add_label":
        emoji = data.get("emoji", "🏷️")
        name = data.get("name", "").strip()
        if not name:
            await update.message.reply_text(fmt.format_error("What should the label be called?"), parse_mode="HTML")
            return
        try:
            label_id = db.add_label(emoji, name)
            await update.message.reply_text(
                f"🏷️ Label created: {escape(emoji)} <b>{escape(name)}</b> (id: {label_id})", parse_mode="HTML",
            )
        except sqlite3.IntegrityError:
            await update.message.reply_text(fmt.format_error(f"Label '{name}' already exists."), parse_mode="HTML")

    elif intent == "edit_label":
        old_name = data.get("old_name", "")
        label = db.get_label_by_name(old_name)
        if not label:
            await update.message.reply_text(fmt.format_error(f"Label '{old_name}' not found."), parse_mode="HTML")
            return
        new_emoji = data.get("new_emoji")
        new_name = data.get("new_name")
        db.update_label(label["id"], emoji=new_emoji, name=new_name)
        display_emoji = escape(new_emoji or label["emoji"])
        display_name = escape(new_name or label["name"])
        await update.message.reply_text(
            f"🏷️ Label updated: {display_emoji} <b>{display_name}</b>", parse_mode="HTML",
        )

    elif intent == "delete_label":
        name = data.get("name", "")
        label = db.get_label_by_name(name)
        if not label:
            await update.message.reply_text(fmt.format_error(f"Label '{name}' not found."), parse_mode="HTML")
            return
        db.delete_label(label["id"])
        await update.message.reply_text(f"🗑️ Label <b>{escape(name)}</b> deleted.", parse_mode="HTML")

    elif intent == "stop_recur":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        if not task["recurrence_rule"]:
            await update.message.reply_text(fmt.format_error("That's not a recurring task."), parse_mode="HTML")
            return
        db.stop_recurrence(task["id"])
        await update.message.reply_text(
            f"🛑 Recurrence stopped for \"{escape(task['description'])}\".", parse_mode="HTML",
        )

    elif intent == "view_task":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        labels = db.get_labels_for_task(task["id"])
        await update.message.reply_text(fmt.format_task_detail(task, labels), parse_mode="HTML")

    elif intent == "update_notes":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        notes = data.get("notes", "")
        db.update_task_notes(task["id"], notes)
        await update.message.reply_text(
            f"📎 Notes updated for \"{escape(task['description'])}\":\n<i>{escape(notes)}</i>", parse_mode="HTML",
        )

    elif intent == "assign_label":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        label_name = data.get("label_name", "")
        label = db.get_label_by_name(label_name)
        if not label:
            await update.message.reply_text(fmt.format_error(f"Label '{label_name}' not found."), parse_mode="HTML")
            return
        db.add_task_label(task["id"], label["id"])
        await update.message.reply_text(
            f"🏷️ {escape(label['emoji'])} <b>{escape(label['name'])}</b> → \"{escape(task['description'])}\"",
            parse_mode="HTML",
        )

    elif intent == "remove_label":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        label_name = data.get("label_name", "")
        label = db.get_label_by_name(label_name)
        if not label:
            await update.message.reply_text(fmt.format_error(f"Label '{label_name}' not found."), parse_mode="HTML")
            return
        db.remove_task_label(task["id"], label["id"])
        await update.message.reply_text(
            f"🏷️ <b>{escape(label['name'])}</b> removed from \"{escape(task['description'])}\"",
            parse_mode="HTML",
        )

    elif intent == "edit_task":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return

        new_desc = data.get("new_description")
        new_date = data.get("new_date")
        new_time = data.get("new_time")
        changes = {}
        if new_desc:
            changes["description"] = new_desc
        if new_date:
            changes["due_date"] = new_date
        if new_time:
            changes["due_time"] = new_time

        if not changes:
            # If reason is move/postpone but no date given, default to tomorrow
            reason = data.get("reason", "edit")
            if reason == "move":
                tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
                changes["due_date"] = tomorrow
                new_date = tomorrow
            else:
                await update.message.reply_text(fmt.format_error("No changes specified."), parse_mode="HTML")
                return

        store_undo(context, "edit", task["id"], task_to_dict(task))
        db.update_task(task["id"], description=new_desc, due_date=new_date, due_time=new_time)
        reason = data.get("reason", "edit")
        await update.message.reply_text(
            fmt.format_task_edited(task["id"], changes, reason=reason,
                                   task_description=task["description"]),
            parse_mode="HTML",
        )

    elif intent == "move_remaining":
        target_date = data.get("target_date")
        if not target_date:
            await update.message.reply_text(
                fmt.format_error("Move to when? Try: \"move remaining tasks to tomorrow\""),
                parse_mode="HTML",
            )
            return
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            await update.message.reply_text(
                fmt.format_error("Couldn't understand the target date."), parse_mode="HTML",
            )
            return

        scope = data.get("scope", "today")
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        if scope == "overdue":
            tasks = db.get_overdue_tasks()
            scope_label = "overdue"
        elif scope == "all":
            today_tasks = db.get_tasks_for_date(today)
            overdue_tasks = db.get_overdue_tasks()
            seen_ids = set()
            tasks = []
            for t in today_tasks + overdue_tasks:
                if t["id"] not in seen_ids:
                    tasks.append(t)
                    seen_ids.add(t["id"])
            scope_label = "pending"
        else:
            tasks = db.get_tasks_for_date(today)
            scope_label = "today's"

        if not tasks:
            await update.message.reply_text(
                f"📋 <b>No {scope_label} pending tasks to move.</b>", parse_mode="HTML",
            )
            return

        # Filter out excluded tasks
        exclude_raw = data.get("exclude") or []
        excluded_ids = set()
        if exclude_raw:
            pos_map = context.application.bot_data.get("task_pos_map", {})
            for exc in exclude_raw:
                if isinstance(exc, int):
                    tid = pos_map.get(exc)
                    if tid:
                        excluded_ids.add(tid)
                elif isinstance(exc, str):
                    kw = exc.lower()
                    for t in tasks:
                        if kw in t["description"].lower():
                            excluded_ids.add(t["id"])

        to_move = [t for t in tasks if t["id"] not in excluded_ids]
        skipped = [t for t in tasks if t["id"] in excluded_ids]

        if not to_move:
            await update.message.reply_text(
                f"📋 <b>All {scope_label} tasks are excluded — nothing to move.</b>", parse_mode="HTML",
            )
            return

        for task in to_move:
            store_undo(context, "edit", task["id"], task_to_dict(task))
            db.update_task(task["id"], due_date=target_date)
        human_date = fmt._humanize_date(target_date)
        skip_msg = ""
        if skipped:
            skip_names = ", ".join(escape(t["description"]) for t in skipped)
            skip_msg = f"\n⏭️ <b>Kept in place:</b> {skip_names}"
        await update.message.reply_text(
            f"📦 <b>Moved {len(to_move)} {scope_label} task(s) to {human_date}.</b>{skip_msg}",
            parse_mode="HTML",
        )

    elif intent == "bulk_done":
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tasks = db.get_tasks_for_date(today)
        if not tasks:
            await update.message.reply_text(
                "📋 <b>No pending tasks for today.</b>", parse_mode="HTML",
            )
            return

        # Filter out excluded tasks
        exclude_raw = data.get("exclude") or []
        excluded_ids = set()
        if exclude_raw:
            pos_map = context.application.bot_data.get("task_pos_map", {})
            for exc in exclude_raw:
                if isinstance(exc, int):
                    # Position reference -> resolve to task ID
                    tid = pos_map.get(exc)
                    if tid:
                        excluded_ids.add(tid)
                elif isinstance(exc, str):
                    # Description keyword match
                    kw = exc.lower()
                    for t in tasks:
                        if kw in t["description"].lower():
                            excluded_ids.add(t["id"])

        to_mark = [t for t in tasks if t["id"] not in excluded_ids]
        skipped = [t for t in tasks if t["id"] in excluded_ids]

        if not to_mark:
            await update.message.reply_text(
                "📋 <b>All tasks are excluded — nothing to mark done.</b>", parse_mode="HTML",
            )
            return

        # Confirm before bulk action if multiple tasks
        pending_bulk = context.user_data.pop("pending_bulk_done", False)
        if len(to_mark) > 1 and not pending_bulk:
            context.user_data["pending_bulk_done"] = True
            context.user_data["bulk_done_excluded_ids"] = excluded_ids
            task_list = "\n".join(f"  • {escape(t['description'])}" for t in to_mark)
            skip_note = ""
            if skipped:
                skip_names = ", ".join(escape(t["description"]) for t in skipped)
                skip_note = f"\n\n⏭️ <b>Skipping:</b> {skip_names}"
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Yes, mark all done", callback_data="bulk_done_confirm"),
                InlineKeyboardButton("❌ Cancel", callback_data="bulk_done_cancel"),
            ]])
            await update.message.reply_text(
                f"⚠️ <b>Mark {len(to_mark)} task(s) as done?</b>\n\n{task_list}{skip_note}",
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            return

        for task in to_mark:
            db.update_task_status(task["id"], "done")
            if task["recurrence_rule"] and task["recurrence_active"]:
                db.create_next_occurrence(task["id"])
        skip_msg = ""
        if skipped:
            skip_names = ", ".join(escape(t["description"]) for t in skipped)
            skip_msg = f"\n⏭️ <b>Skipped:</b> {skip_names}"
        await update.message.reply_text(
            f"🎉 <b>All done!</b> Marked {len(to_mark)} task(s) as completed.{skip_msg}", parse_mode="HTML",
        )

    elif intent == "snooze":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        store_undo(context, "edit", task["id"], task_to_dict(task))
        duration = data.get("duration", "1h")
        now = datetime.now(TIMEZONE)
        if duration == "tomorrow":
            tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
            db.carry_over_task(task["id"], tomorrow, task["due_time"])
            await update.message.reply_text(
                fmt.format_snoozed(task["id"], tomorrow, task["due_time"]), parse_mode="HTML",
            )
        else:
            # Parse hours from duration like "1h", "2h", "3h"
            import re
            m = re.match(r"(\d+)h", duration)
            hours = int(m.group(1)) if m else 1
            if not task["due_time"]:
                # No time set — move to tomorrow instead
                tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
                db.carry_over_task(task["id"], tomorrow, None)
                await update.message.reply_text(
                    fmt.format_snoozed(task["id"], tomorrow, None), parse_mode="HTML",
                )
            else:
                # Snooze from NOW if task is overdue, otherwise from due time
                due_dt = datetime.strptime(
                    f"{task['due_date']} {task['due_time']}", "%Y-%m-%d %H:%M"
                ).replace(tzinfo=TIMEZONE)
                base_dt = max(due_dt, now)
                new_dt = base_dt + timedelta(hours=hours)
                new_date = new_dt.strftime("%Y-%m-%d")
                new_time = new_dt.strftime("%H:%M")
                db.carry_over_task(task["id"], new_date, new_time)
                await update.message.reply_text(
                    fmt.format_snoozed(task["id"], new_date, new_time), parse_mode="HTML",
                )

    elif intent == "greeting":
        greet_type = data.get("type", "hello")
        if greet_type == "thanks":
            await update.message.reply_text(
                "😊 <b>You're welcome!</b> Let me know if you need anything else.",
                parse_mode="HTML",
            )
        elif greet_type == "goodbye":
            await update.message.reply_text(
                "👋 <b>See you later!</b> I'll keep your reminders running.",
                parse_mode="HTML",
            )
        else:
            today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
            tasks = db.get_tasks_for_date(today)
            task_info = f" You have <b>{len(tasks)}</b> task(s) for today." if tasks else " No tasks for today yet."
            await update.message.reply_text(
                f"👋 <b>Hey there!</b>{task_info}\n\n<i>Send me a task or ask anything!</i>",
                parse_mode="HTML",
            )

    elif intent == "clear":
        scope = data.get("scope", "ask")
        # Map legacy "all" to "all_tasks"
        if scope == "all":
            scope = "all_tasks"

        # Resolve excluded tasks
        exclude_raw = data.get("exclude") or []
        excluded_ids = set()
        if exclude_raw and scope in ("today", "overdue", "upcoming"):
            today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
            if scope == "overdue":
                tasks = db.get_overdue_tasks()
            else:
                tasks = db.get_tasks_for_date(today)  # today or upcoming (starts from today)
            pos_map = context.application.bot_data.get("task_pos_map", {})
            for exc in exclude_raw:
                if isinstance(exc, int):
                    tid = pos_map.get(exc)
                    if tid:
                        excluded_ids.add(tid)
                elif isinstance(exc, str):
                    kw = exc.lower()
                    for t in tasks:
                        if kw in t["description"].lower():
                            excluded_ids.add(t["id"])

        if excluded_ids:
            context.user_data["clear_excluded_ids"] = excluded_ids
        await _send_clear_confirmation(update.message, scope, excluded_ids=excluded_ids)

    elif intent == "undo":
        return await undo_command(update, context)

    elif intent == "backup":
        return await backup_command(update, context)

    elif intent == "compound":
        actions = data.get("actions", [])
        # Snapshot the position map so all actions resolve against the original list
        original_pos_map = dict(context.application.bot_data.get("task_pos_map", {}))
        for action in actions:
            action_intent = action.get("intent", "unknown")
            # Restore original pos_map before each action so positions stay consistent
            context.application.bot_data["task_pos_map"] = dict(original_pos_map)
            if action_intent == "add_task":
                try:
                    due_date = action.get("due_date", "")
                    due_time = action.get("due_time")
                    datetime.strptime(due_date, "%Y-%m-%d")
                    if due_time:
                        try:
                            datetime.strptime(due_time, "%H:%M")
                        except (ValueError, TypeError):
                            due_time = None
                    parsed = ParsedTask(
                        description=action.get("description", "").strip(),
                        due_date=due_date,
                        due_time=due_time,
                        confidence=action.get("confidence", 1.0),
                        recurrence_rule=action.get("recurrence_rule"),
                        label_names=action.get("labels", []),
                        notes=action.get("notes"),
                    )
                    if parsed.description:
                        await _add_task_and_respond(update, context, parsed)
                except (ValueError, TypeError):
                    pass
            else:
                await _route_intent(update, context, action, action_intent)

    elif intent == "routine":
        action = data.get("action", "list")
        if action == "list":
            items = db.get_all_routine_items()
            await update.message.reply_text(fmt.format_routine_list(items), parse_mode="HTML")
        elif action == "add":
            desc = data.get("description", "").strip()
            if not desc:
                await update.message.reply_text(fmt.format_error("What should the routine item be?"), parse_mode="HTML")
                return
            target_time = data.get("target_time")
            db.add_routine_item(desc, target_time)
            time_str = f" at {target_time}" if target_time else ""
            await update.message.reply_text(
                f"🌅 <b>Added to routine:</b> {escape(desc)}{time_str}", parse_mode="HTML",
            )
        elif action == "remove":
            desc = data.get("description", "").strip()
            if not desc:
                await update.message.reply_text(fmt.format_error("Which routine item to remove?"), parse_mode="HTML")
                return
            item = db.get_routine_item_by_description(desc)
            if item:
                db.delete_routine_item(item["id"])
                await update.message.reply_text(
                    f"🗑️ Removed from routine: <b>{escape(item['description'])}</b>", parse_mode="HTML",
                )
            else:
                await update.message.reply_text(
                    fmt.format_error(f"No routine item matching \"{desc}\"."), parse_mode="HTML",
                )

    elif intent == "help":
        return await help_command(update, context)

    elif intent == "negative":
        await update.message.reply_text(
            "👍 <b>OK, no worries.</b> Let me know if you need anything.",
            parse_mode="HTML",
        )

    elif intent == "skip_task":
        # If no task reference given, try last reminded task as fallback
        if not data.get("task_id") and not data.get("task_description"):
            last_reminded = context.application.bot_data.get("last_reminded_task_id")
            if last_reminded:
                fallback_task = db.get_task(last_reminded)
                if fallback_task and fallback_task["status"] == "pending":
                    data["task_id"] = last_reminded

        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return
        # Offer to carry over to tomorrow
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📅 Move to tomorrow", callback_data=f"skip_tomorrow_{task['id']}"),
            InlineKeyboardButton("🗑️ Delete it", callback_data=f"skip_delete_{task['id']}"),
            InlineKeyboardButton("🤷 Leave it", callback_data="skip_leave"),
        ]])
        await update.message.reply_text(
            f"📝 <b>Got it</b> — \"{escape(task['description'])}\" isn't done yet.\n\n"
            f"What would you like to do with it?",
            parse_mode="HTML",
            reply_markup=keyboard,
        )

    elif intent == "set_reminder":
        task = await _resolve_or_ask(update, data, context, intent=intent)
        if not task:
            return

        reminder_type = data.get("reminder_type")
        if reminder_type not in ("absolute", "offset", "repeating"):
            await update.message.reply_text(
                fmt.format_error("Couldn't understand that reminder type."),
                parse_mode="HTML",
            )
            return

        if reminder_type == "absolute":
            time_str = data.get("time")
            date_str = data.get("date") or task["due_date"]
            if not time_str:
                await update.message.reply_text(
                    fmt.format_error("What time should I remind you?"),
                    parse_mode="HTML",
                )
                return
            fire_at = f"{date_str} {time_str}"
            try:
                fire_dt = datetime.strptime(fire_at, "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
            except (ValueError, TypeError):
                await update.message.reply_text(
                    fmt.format_error("Couldn't understand that date/time."),
                    parse_mode="HTML",
                )
                return
            if fire_dt <= datetime.now(TIMEZONE):
                await update.message.reply_text(
                    fmt.format_error("That time has already passed."),
                    parse_mode="HTML",
                )
                return
            db.add_custom_reminder(task["id"], "absolute", fire_at=fire_at)
            await update.message.reply_text(
                fmt.format_custom_reminder_set(task, "absolute", fire_at=fire_at),
                parse_mode="HTML",
            )

        elif reminder_type == "offset":
            offset = data.get("offset_minutes")
            if not offset or offset <= 0:
                await update.message.reply_text(
                    fmt.format_error("How long before the task should I remind you?"),
                    parse_mode="HTML",
                )
                return
            if not task["due_time"]:
                await update.message.reply_text(
                    fmt.format_error("This task has no due time. Set a time first or use an absolute reminder."),
                    parse_mode="HTML",
                )
                return
            db.add_custom_reminder(task["id"], "offset", offset_minutes=offset)
            await update.message.reply_text(
                fmt.format_custom_reminder_set(task, "offset", offset_minutes=offset),
                parse_mode="HTML",
            )

        elif reminder_type == "repeating":
            interval = data.get("interval_minutes")
            if not interval or interval <= 0:
                await update.message.reply_text(
                    fmt.format_error("How often should I remind you?"),
                    parse_mode="HTML",
                )
                return
            if not task["due_time"]:
                await update.message.reply_text(
                    fmt.format_error("This task has no due time. Repeating reminders need a deadline."),
                    parse_mode="HTML",
                )
                return
            db.add_custom_reminder(task["id"], "repeating", interval_minutes=interval)
            await update.message.reply_text(
                fmt.format_custom_reminder_set(task, "repeating", interval_minutes=interval),
                parse_mode="HTML",
            )

    else:
        await update.message.reply_text(fmt.format_not_understood(), parse_mode="HTML")


async def _end_morning_session(update, context):
    """End the morning prompt session and show summary."""
    context.application.bot_data["morning_prompt_active"] = False
    added_ids = context.application.bot_data.pop("morning_prompt_tasks", [])

    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    all_tasks = db.get_tasks_for_date(today)

    added_tasks = []
    for tid in added_ids:
        t = db.get_task(tid)
        if t:
            added_tasks.append(t)

    msg = fmt.format_morning_summary(added_tasks, all_tasks)
    await update.message.reply_text(msg, parse_mode="HTML")
