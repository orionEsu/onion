import functools
import logging
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


def _remaining_today_summary() -> str:
    """Return a short summary of remaining tasks for today, or empty string."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    remaining = db.get_tasks_for_date(today)
    if not remaining:
        return "\n\n🎉 <i>All done for today!</i>"
    labels_map = _build_labels_map(remaining)
    return "\n\n" + fmt.format_task_list(
        f"📋 <b>Remaining ({len(remaining)})</b>", remaining, labels_map,
    )


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


def _is_past_time(due_date: str, due_time: str | None) -> bool:
    """Check if a task's date+time is already in the past."""
    if not due_time:
        return False
    now = datetime.now(TIMEZONE)
    today = now.strftime("%Y-%m-%d")
    if due_date != today:
        return False
    try:
        task_dt = datetime.strptime(f"{due_date} {due_time}", "%Y-%m-%d %H:%M").replace(tzinfo=TIMEZONE)
        return task_dt < now
    except ValueError:
        return False


async def _add_task_and_respond(update, context, parsed):
    """Add a task from ParsedTask and send styled response + label prompt."""
    # Check if time is already past
    if _is_past_time(parsed.due_date, parsed.due_time):
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        context.user_data["pending_past_task"] = parsed
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📅 Move to tomorrow", callback_data=f"past_task_tomorrow"),
            InlineKeyboardButton("✅ Keep today", callback_data=f"past_task_keep"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"past_task_cancel"),
        ]])
        await update.message.reply_text(
            f"⏰ <b>Heads up!</b> {parsed.due_time} today has already passed.\n\n"
            f"📝 \"{escape(parsed.description)}\"\n\n"
            f"Move it to <b>tomorrow ({tomorrow})</b>, keep it for today, or cancel?",
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
            fmt.format_label_prompt(task_id),
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
        fmt.format_label_prompt(task_id), parse_mode="HTML", reply_markup=keyboard,
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
    msg = fmt.format_task_list(header, tasks, labels_map)

    if overdue:
        overdue_labels = _build_labels_map(overdue)
        msg += "\n\n" + fmt.format_overdue_warning(overdue, overdue_labels)

    await update.message.reply_text(msg, parse_mode="HTML")


@authorized
async def upcoming_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = db.get_upcoming_tasks()
    labels_map = _build_labels_map(tasks)
    msg = fmt.format_task_list("📅 <b>Upcoming Tasks</b>", tasks, labels_map, show_date=True)
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

    store_undo(context, "done", task_id, task_to_dict(task))
    db.update_task_status(task_id, "done")

    # Handle recurrence
    next_id = None
    if task["recurrence_rule"] and task["recurrence_active"]:
        next_id = db.create_next_occurrence(task_id)

    msg = fmt.format_task_done(task, next_id)
    msg += _remaining_today_summary()
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

    store_undo(context, "delete", task_id, task_to_dict(task))
    db.delete_task(task_id)
    msg = f"🗑️ Task #{task_id} deleted."
    msg += _remaining_today_summary()
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
    except Exception:
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
    await update.message.reply_text(
        fmt.format_task_list(title, tasks, labels_map, show_date=True),
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
    await update.message.reply_text(fmt.format_task_edited(task_id, changes), parse_mode="HTML")


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


async def _show_history(update, period: str):
    """Shared logic for history display."""
    today = datetime.now(TIMEZONE)
    today_str = today.strftime("%Y-%m-%d")

    if period == "today":
        start, end, label = today_str, today_str, "Today"
    elif period == "week":
        start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end, label = today_str, "Past 7 Days"
    elif period == "month":
        start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        end, label = today_str, "Past 30 Days"
    elif period == "all":
        start, end, label = None, None, "All Time"
    else:
        await update.message.reply_text(
            fmt.format_error("Usage: /history [today|week|month|all]"), parse_mode="HTML",
        )
        return

    tasks = db.get_completed_tasks(start, end)
    labels_map = _build_labels_map(tasks)
    await update.message.reply_text(fmt.format_history(tasks, label, labels_map), parse_mode="HTML")


@authorized
async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    period = args[0].lower() if args else "week"
    await _show_history(update, period)


@authorized
async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import sqlite3
    import tempfile
    from bot.config import DB_PATH

    import os
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


VALID_CLEAR_SCOPES = {"today", "upcoming", "all"}

@authorized
async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    scope = args[0].lower() if args else ""
    if scope not in VALID_CLEAR_SCOPES:
        await update.message.reply_text(
            fmt.format_error("Usage: /clear <today|upcoming|all>"), parse_mode="HTML",
        )
        return

    labels = {"today": "today's pending tasks", "upcoming": "all upcoming tasks", "all": "ALL tasks (including completed)"}
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Yes, clear them", callback_data=f"clear_confirm_{scope}"),
        InlineKeyboardButton("Cancel", callback_data="clear_cancel"),
    ]])
    await update.message.reply_text(
        f"⚠️ <b>Are you sure?</b>\n\nThis will permanently delete <b>{labels[scope]}</b>.",
        parse_mode="HTML",
        reply_markup=keyboard,
    )


# ── Natural language handler ─────────────────────────────────────

@authorized
async def handle_natural_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Check if we're awaiting a carryover date
    if context.user_data.get("awaiting_carry_date"):
        task_id = context.user_data.pop("awaiting_carry_date")
        text = (update.message.text or "").strip()
        if not text:
            await update.message.reply_text(
                fmt.format_error("Please send a date. Format: YYYY-MM-DD or YYYY-MM-DD HH:MM"),
                parse_mode="HTML",
            )
            context.user_data["awaiting_carry_date"] = task_id
            return
        tokens = text.split()
        try:
            new_date = tokens[0]
            datetime.strptime(new_date, "%Y-%m-%d")
            new_time = tokens[1] if len(tokens) > 1 else None
            if new_time:
                datetime.strptime(new_time, "%H:%M")
            db.carry_over_task(task_id, new_date, new_time)
            await update.message.reply_text(
                fmt.format_review_carried(task_id, new_date), parse_mode="HTML",
            )
        except (ValueError, IndexError):
            await update.message.reply_text(
                fmt.format_error("Invalid format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM"),
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


def _resolve_task(data: dict):
    """Resolve a task from task_id or task_description.
    Returns (task, error_msg, ambiguous_list).
    - Single match: (task, None, None)
    - No match/error: (None, error_msg, None)
    - Multiple matches: (None, None, [tasks])
    """
    task_id = data.get("task_id")
    task_desc = data.get("task_description")

    if task_id:
        task = db.get_task(int(task_id))
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


async def _resolve_or_ask(update, data: dict):
    """Resolve task or send disambiguation/error. Returns task or None."""
    task, err, ambiguous = _resolve_task(data)
    if err:
        await update.message.reply_text(fmt.format_error(err), parse_mode="HTML")
        return None
    if ambiguous:
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
            await update.message.reply_text(
                fmt.format_task_list(title, tasks, labels_map, show_date=True), parse_mode="HTML",
            )
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
        else:
            return await tasks_command(update, context)

    elif intent == "done":
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        store_undo(context, "done", task["id"], task_to_dict(task))
        db.update_task_status(task["id"], "done")
        next_id = None
        if task["recurrence_rule"] and task["recurrence_active"]:
            next_id = db.create_next_occurrence(task["id"])
        msg = fmt.format_task_done(task, next_id)
        msg += _remaining_today_summary()
        await update.message.reply_text(msg, parse_mode="HTML")

    elif intent == "delete":
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        store_undo(context, "delete", task["id"], task_to_dict(task))
        db.delete_task(task["id"])
        msg = f"🗑️ Task #{task['id']} deleted."
        msg += _remaining_today_summary()
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
        except Exception:
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
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        if not task.get("recurrence_rule"):
            await update.message.reply_text(fmt.format_error("That's not a recurring task."), parse_mode="HTML")
            return
        db.stop_recurrence(task["id"])
        await update.message.reply_text(f"🛑 Recurrence stopped for task #{task['id']}.", parse_mode="HTML")

    elif intent == "view_task":
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        labels = db.get_labels_for_task(task["id"])
        await update.message.reply_text(fmt.format_task_detail(task, labels), parse_mode="HTML")

    elif intent == "update_notes":
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        notes = data.get("notes", "")
        db.update_task_notes(task["id"], notes)
        await update.message.reply_text(
            f"📎 Notes updated for task <b>#{task['id']}</b>:\n<i>{escape(notes)}</i>", parse_mode="HTML",
        )

    elif intent == "assign_label":
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        label_name = data.get("label_name", "")
        label = db.get_label_by_name(label_name)
        if not label:
            await update.message.reply_text(fmt.format_error(f"Label '{label_name}' not found."), parse_mode="HTML")
            return
        db.add_task_label(task["id"], label["id"])
        await update.message.reply_text(
            f"🏷️ {escape(label['emoji'])} <b>{escape(label['name'])}</b> assigned to task <b>#{task['id']}</b>",
            parse_mode="HTML",
        )

    elif intent == "remove_label":
        task = await _resolve_or_ask(update, data)
        if not task:
            return
        label_name = data.get("label_name", "")
        label = db.get_label_by_name(label_name)
        if not label:
            await update.message.reply_text(fmt.format_error(f"Label '{label_name}' not found."), parse_mode="HTML")
            return
        db.remove_task_label(task["id"], label["id"])
        await update.message.reply_text(
            f"🏷️ <b>{escape(label['name'])}</b> removed from task <b>#{task['id']}</b>",
            parse_mode="HTML",
        )

    elif intent == "edit_task":
        task = await _resolve_or_ask(update, data)
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
            await update.message.reply_text(fmt.format_error("No changes specified."), parse_mode="HTML")
            return

        store_undo(context, "edit", task["id"], task_to_dict(task))
        db.update_task(task["id"], description=new_desc, due_date=new_date, due_time=new_time)
        reason = data.get("reason", "edit")
        await update.message.reply_text(
            fmt.format_task_edited(task["id"], changes, reason=reason), parse_mode="HTML",
        )

    elif intent == "clear":
        scope = data.get("scope", "")
        if scope not in VALID_CLEAR_SCOPES:
            await update.message.reply_text(
                fmt.format_error("Clear what? Say \"clear today\", \"clear upcoming\", or \"clear all\"."),
                parse_mode="HTML",
            )
            return
        labels = {"today": "today's pending tasks", "upcoming": "all upcoming tasks", "all": "ALL tasks (including completed)"}
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Yes, clear them", callback_data=f"clear_confirm_{scope}"),
            InlineKeyboardButton("Cancel", callback_data="clear_cancel"),
        ]])
        await update.message.reply_text(
            f"⚠️ <b>Are you sure?</b>\n\nThis will permanently delete <b>{labels[scope]}</b>.",
            parse_mode="HTML",
            reply_markup=keyboard,
        )

    elif intent == "undo":
        return await undo_command(update, context)

    elif intent == "backup":
        return await backup_command(update, context)

    elif intent == "compound":
        actions = data.get("actions", [])
        for action in actions:
            action_intent = action.get("intent", "unknown")
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

    elif intent == "help":
        return await help_command(update, context)

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
