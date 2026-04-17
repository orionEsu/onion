import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.config import AUTHORIZED_USER_ID, TIMEZONE
from bot import database as db
from bot import formatting as fmt
from bot.utils import store_undo, task_to_dict

logger = logging.getLogger(__name__)


def _build_routine_keyboard(items: list, completed_ids: set) -> InlineKeyboardMarkup:
    """Build per-item toggle buttons for routine checklist."""
    buttons = []
    for item in items:
        check = "✅" if item["id"] in completed_ids else "⬜"
        time_str = f" ({item['target_time']})" if item["target_time"] else ""
        buttons.append([InlineKeyboardButton(
            f"{check} {item['description']}{time_str}",
            callback_data=f"routine_check_{item['id']}",
        )])
    return InlineKeyboardMarkup(buttons)


async def send_morning_prompt(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled at 7 AM. Sends varied greeting + week preview + existing tasks."""
    try:
        # Auto-generate today's recurring task instances
        db.generate_recurring_for_today()

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        existing = db.get_tasks_for_date(today)

        msg = fmt.format_morning_prompt()

        # Week preview
        week_counts = db.get_week_task_counts(today)
        # Remove today from preview (already shown separately)
        week_counts.pop(today, None)
        if week_counts:
            msg += "\n\n" + fmt.format_week_preview(week_counts)

        if existing:
            labels_map = db.get_labels_for_tasks([t["id"] for t in existing])
            text, _ = fmt.format_task_list("📋 <b>Already on your plate</b>", existing, labels_map)
            msg += "\n\n" + text

        # Show overdue tasks if any
        overdue = db.get_overdue_tasks()
        if overdue:
            overdue_labels = db.get_labels_for_tasks([t["id"] for t in overdue])
            msg += "\n\n" + fmt.format_overdue_warning(overdue, overdue_labels)

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ I'm done adding", callback_data="morning_done"),
        ]])

        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=msg,
            parse_mode="HTML",
            reply_markup=keyboard,
        )

        context.application.bot_data["morning_prompt_active"] = True
        context.application.bot_data["morning_prompt_tasks"] = []

        # Send routine checklist as a separate message (if items exist)
        routine_items = db.get_all_routine_items()
        if routine_items:
            completed_ids = db.get_routine_completions_for_date(today)
            checklist_text = fmt.format_routine_checklist(routine_items, completed_ids)
            routine_kb = _build_routine_keyboard(routine_items, completed_ids)
            await context.bot.send_message(
                chat_id=AUTHORIZED_USER_ID,
                text=checklist_text,
                parse_mode="HTML",
                reply_markup=routine_kb,
            )
    except Exception:
        logger.exception("send_morning_prompt job failed")


async def send_daily_review(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled at 9 PM. Sends review with inline buttons."""
    try:
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tasks = db.get_unreviewed_tasks_for_date(today)

        if not tasks:
            await context.bot.send_message(
                chat_id=AUTHORIZED_USER_ID,
                text=fmt.format_no_tasks_review(),
                parse_mode="HTML",
            )
            return

        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=fmt.format_daily_review_header(),
            parse_mode="HTML",
        )

        labels_map = db.get_labels_for_tasks([t["id"] for t in tasks])
        for task in tasks:
            tid = task["id"]
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Done", callback_data=f"review_done_{tid}"),
                InlineKeyboardButton("❌ Not done", callback_data=f"review_undone_{tid}"),
            ]])
            await context.bot.send_message(
                chat_id=AUTHORIZED_USER_ID,
                text=fmt.format_review_task(task, labels_map.get(tid, [])),
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            db.mark_reviewed(tid)
    except Exception:
        logger.exception("send_daily_review job failed")


def _parse_int(s: str) -> int | None:
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


async def _handle_snooze(query, data: str):
    """Handle snooze_1h_, snooze_3h_, snooze_tomorrow_ callbacks."""
    if data.startswith("snooze_tomorrow_"):
        task_id = _parse_int(data.removeprefix("snooze_tomorrow_"))
        if task_id is None:
            return
        task = db.get_task(task_id)
        if not task:
            return
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        db.carry_over_task(task_id, tomorrow, task["due_time"])
        await query.edit_message_text(
            fmt.format_snoozed(task_id, tomorrow, task["due_time"]), parse_mode="HTML",
        )
    elif data.startswith("snooze_1h_") or data.startswith("snooze_3h_"):
        if data.startswith("snooze_1h_"):
            task_id = _parse_int(data.removeprefix("snooze_1h_"))
            hours = 1
        else:
            task_id = _parse_int(data.removeprefix("snooze_3h_"))
            hours = 3
        if task_id is None:
            return
        task = db.get_task(task_id)
        if not task or not task["due_time"]:
            return
        due_dt = datetime.strptime(
            f"{task['due_date']} {task['due_time']}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=TIMEZONE)
        # Snooze from now if task is already overdue, otherwise from due time
        base_dt = max(due_dt, datetime.now(TIMEZONE))
        new_dt = base_dt + timedelta(hours=hours)
        new_date = new_dt.strftime("%Y-%m-%d")
        new_time = new_dt.strftime("%H:%M")
        db.carry_over_task(task_id, new_date, new_time)
        await query.edit_message_text(
            fmt.format_snoozed(task_id, new_date, new_time), parse_mode="HTML",
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != AUTHORIZED_USER_ID:
        return

    query = update.callback_query
    await query.answer()
    data = query.data

    # ── Review callbacks ──

    if data.startswith("review_done_"):
        task_id = _parse_int(data.removeprefix("review_done_"))
        if task_id is None:
            return
        task = db.get_task(task_id)
        if task:
            store_undo(context, "done", task_id, task_to_dict(task))
        db.update_task_status(task_id, "done")

        next_id = None
        if task and task["recurrence_rule"] and task["recurrence_active"]:
            next_id = db.create_next_occurrence(task_id)

        msg = fmt.format_review_done(task_id)
        if next_id:
            msg += f"\n🔄 Next → <b>#{next_id}</b>"
        await query.edit_message_text(msg, parse_mode="HTML")

    elif data.startswith("review_undone_"):
        task_id = _parse_int(data.removeprefix("review_undone_"))
        if task_id is None:
            return
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📅 Tomorrow", callback_data=f"carry_tomorrow_{task_id}"),
            InlineKeyboardButton("🗓️ Pick a date", callback_data=f"carry_pick_{task_id}"),
            InlineKeyboardButton("🗑️ Drop it", callback_data=f"carry_drop_{task_id}"),
        ]])
        await query.edit_message_text(
            f"📌 <b>#{task_id}</b> not done. Carry over to:",
            parse_mode="HTML",
            reply_markup=keyboard,
        )

    elif data.startswith("carry_tomorrow_"):
        task_id = _parse_int(data.removeprefix("carry_tomorrow_"))
        if task_id is None:
            return
        task = db.get_task(task_id)
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        db.carry_over_task(task_id, tomorrow, task["due_time"] if task else None)
        await query.edit_message_text(
            fmt.format_review_carried(task_id, tomorrow), parse_mode="HTML",
        )

    elif data.startswith("carry_pick_"):
        task_id = _parse_int(data.removeprefix("carry_pick_"))
        if task_id is None:
            return
        context.user_data["awaiting_carry_date"] = task_id
        await query.edit_message_text(
            f"📅 Send the new date for task <b>#{task_id}</b>\n"
            f"<i>Format: YYYY-MM-DD or YYYY-MM-DD HH:MM</i>",
            parse_mode="HTML",
        )

    elif data.startswith("carry_drop_"):
        task_id = _parse_int(data.removeprefix("carry_drop_"))
        if task_id is None:
            return
        task = db.get_task(task_id)
        if task:
            store_undo(context, "cancel", task_id, task_to_dict(task))
        db.update_task_status(task_id, "cancelled")
        await query.edit_message_text(
            fmt.format_review_dropped(task_id), parse_mode="HTML",
        )

    # ── Task confirmation callbacks ──

    elif data == "confirm_add":
        parsed = context.user_data.pop("pending_task", None)
        if parsed:
            task_id = db.add_task(
                parsed.description, parsed.due_date, parsed.due_time,
                recurrence_rule=parsed.recurrence_rule,
                notes=parsed.notes,
            )
            # Assign labels from NLP
            labels = []
            for name in parsed.label_names:
                label = db.get_label_by_name(name)
                if label:
                    db.add_task_label(task_id, label["id"])
                    labels.append(label)

            msg = fmt.format_task_added(
                task_id, parsed.description, parsed.due_date, parsed.due_time,
                parsed.recurrence_rule, labels,
            )
            await query.edit_message_text(msg, parse_mode="HTML")

    elif data == "cancel_add":
        context.user_data.pop("pending_task", None)
        await query.edit_message_text("❌ Cancelled.", parse_mode="HTML")

    # ── Past-time task callbacks ──

    elif data in ("past_task_tomorrow", "past_task_cancel"):
        parsed = context.user_data.pop("pending_past_task", None)
        if not parsed:
            await query.edit_message_text("⏰ No pending task.", parse_mode="HTML")
            return

        if data == "past_task_cancel":
            await query.edit_message_text("❌ Cancelled.", parse_mode="HTML")
            return

        if data == "past_task_tomorrow":
            tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
            parsed.due_date = tomorrow
            parsed.due_time = None  # Clear past time when moving to tomorrow

        task_id = db.add_task(
            parsed.description, parsed.due_date, parsed.due_time,
            recurrence_rule=parsed.recurrence_rule,
            notes=parsed.notes,
        )

        # Auto-assign labels
        labels = []
        for name in parsed.label_names:
            label = db.get_label_by_name(name)
            if label:
                db.add_task_label(task_id, label["id"])
                labels.append(label)

        msg = fmt.format_task_added(
            task_id, parsed.description, parsed.due_date, parsed.due_time,
            parsed.recurrence_rule, labels, notes=parsed.notes,
        )
        await query.edit_message_text(msg, parse_mode="HTML")

    # ── Label selection callbacks ──

    elif data.startswith("label_toggle_"):
        parts = data.removeprefix("label_toggle_").split("_")
        if len(parts) < 2:
            return
        task_id, label_id = _parse_int(parts[0]), _parse_int(parts[1])
        if task_id is None or label_id is None:
            return

        # Toggle: check if already assigned
        current_labels = db.get_labels_for_task(task_id)
        current_ids = {l["id"] for l in current_labels}

        if label_id in current_ids:
            db.remove_task_label(task_id, label_id)
            current_ids.discard(label_id)
        else:
            db.add_task_label(task_id, label_id)
            current_ids.add(label_id)

        # Rebuild keyboard with updated selections
        all_labels = db.get_all_labels()
        buttons = []
        row = []
        for l in all_labels:
            check = "✓ " if l["id"] in current_ids else ""
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

        await query.edit_message_reply_markup(InlineKeyboardMarkup(buttons))

    elif data.startswith("label_done_"):
        task_id = _parse_int(data.removeprefix("label_done_"))
        if task_id is None:
            return
        labels = db.get_labels_for_task(task_id)
        if labels:
            label_str = " ".join(f"{l['emoji']} {l['name']}" for l in labels)
            await query.edit_message_text(
                f"🏷️ Task <b>#{task_id}</b> labeled: {label_str}",
                parse_mode="HTML",
            )
        else:
            await query.edit_message_text(
                f"🏷️ No labels assigned to task <b>#{task_id}</b>.",
                parse_mode="HTML",
            )

    elif data.startswith("label_skip_"):
        task_id = _parse_int(data.removeprefix("label_skip_"))
        if task_id is None:
            return
        await query.edit_message_text(
            f"⏩ Skipped labeling for task <b>#{task_id}</b>.",
            parse_mode="HTML",
        )

    # ── Routine callbacks ──

    elif data.startswith("routine_check_"):
        item_id = _parse_int(data.removeprefix("routine_check_"))
        if item_id is None:
            return
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        completed_ids = db.get_routine_completions_for_date(today)

        # Toggle
        if item_id in completed_ids:
            db.uncomplete_routine_item(item_id, today)
            completed_ids.discard(item_id)
        else:
            db.complete_routine_item(item_id, today)
            completed_ids.add(item_id)

        # Rebuild checklist message + keyboard
        routine_items = db.get_all_routine_items()
        checklist_text = fmt.format_routine_checklist(routine_items, completed_ids)
        routine_kb = _build_routine_keyboard(routine_items, completed_ids)
        await query.edit_message_text(checklist_text, parse_mode="HTML", reply_markup=routine_kb)

        # Congrats if all complete (once per day)
        congrats_key = f"routine_congrats_{today}"
        if db.is_routine_all_complete(today) and not context.application.bot_data.get(congrats_key):
            context.application.bot_data[congrats_key] = True
            await context.bot.send_message(
                chat_id=AUTHORIZED_USER_ID,
                text="🎉 <b>Morning routine complete!</b> Great start to the day! 💪",
                parse_mode="HTML",
            )

    # ── Undo done callbacks ──

    elif data.startswith("undo_done_"):
        if data == "undo_done_cancel":
            await query.edit_message_text("👍 <b>OK, no changes made.</b>", parse_mode="HTML")
        else:
            task_id = _parse_int(data.removeprefix("undo_done_"))
            if task_id is None:
                return
            task = db.get_task(task_id)
            if not task:
                await query.edit_message_text("Task not found.", parse_mode="HTML")
                return
            store_undo(context, "done", task_id, task_to_dict(task))
            db.update_task_status(task_id, "pending")
            await query.edit_message_text(
                f"↩️ <b>\"{task['description']}\"</b> marked back as pending.",
                parse_mode="HTML",
            )

    # ── Snooze callbacks ──

    elif data.startswith("snooze_"):
        await _handle_snooze(query, data)

    # ── Morning prompt done ──

    elif data == "morning_done":
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
        await query.edit_message_text(msg, parse_mode="HTML")

    # ── Clear confirmation ──

    elif data.startswith("clear_confirm_"):
        scope = data.removeprefix("clear_confirm_")
        valid = ("today", "overdue", "upcoming", "all_tasks", "all_labels", "everything")
        if scope not in valid:
            return
        excluded_ids = context.user_data.pop("clear_excluded_ids", set())
        if excluded_ids and scope in ("today", "overdue", "upcoming"):
            count = db.clear_tasks_except(scope, excluded_ids)
        else:
            count = db.clear_tasks(scope)
        scope_labels = {
            "today": "today's", "overdue": "overdue",
            "upcoming": "upcoming",
            "all_tasks": "all", "all_labels": "all",
            "everything": "all",
        }
        item = "item(s)" if scope in ("everything",) else (
            "label(s)" if scope == "all_labels" else "task(s)"
        )
        await query.edit_message_text(
            f"🧹 <b>Cleared {count} {scope_labels[scope]} {item}.</b>", parse_mode="HTML",
        )

    elif data == "clear_cancel":
        context.user_data.pop("clear_excluded_ids", None)
        await query.edit_message_text("👍 <b>Clear cancelled.</b> Nothing was deleted.", parse_mode="HTML")

    # ── Bulk done confirmation ──

    elif data == "bulk_done_confirm":
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        tasks = db.get_tasks_for_date(today)
        if not tasks:
            await query.edit_message_text(
                "📋 <b>No pending tasks for today.</b>", parse_mode="HTML",
            )
            return
        excluded_ids = context.user_data.pop("bulk_done_excluded_ids", set())
        to_mark = [t for t in tasks if t["id"] not in excluded_ids]
        if not to_mark:
            await query.edit_message_text(
                "📋 <b>No tasks to mark done.</b>", parse_mode="HTML",
            )
            return
        for task in to_mark:
            db.update_task_status(task["id"], "done")
            if task["recurrence_rule"] and task["recurrence_active"]:
                db.create_next_occurrence(task["id"])
        await query.edit_message_text(
            f"🎉 <b>All done!</b> Marked {len(to_mark)} task(s) as completed.", parse_mode="HTML",
        )

    elif data == "bulk_done_cancel":
        context.user_data.pop("pending_bulk_done", None)
        context.user_data.pop("bulk_done_excluded_ids", None)
        await query.edit_message_text("👍 <b>OK, no changes made.</b>", parse_mode="HTML")

    # ── Skip task actions ──

    elif data.startswith("skip_tomorrow_"):
        task_id = int(data.removeprefix("skip_tomorrow_"))
        task = db.get_task(task_id)
        if not task:
            await query.edit_message_text("❌ Task not found.", parse_mode="HTML")
            return
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        db.carry_over_task(task_id, tomorrow, task["due_time"])
        from html import escape
        await query.edit_message_text(
            f"📅 <b>Moved to tomorrow:</b> \"{escape(task['description'])}\"",
            parse_mode="HTML",
        )

    elif data.startswith("skip_delete_"):
        task_id = int(data.removeprefix("skip_delete_"))
        task = db.get_task(task_id)
        if not task:
            await query.edit_message_text("❌ Task not found.", parse_mode="HTML")
            return
        from html import escape
        db.delete_task(task_id)
        await query.edit_message_text(
            f"🗑️ <b>Deleted:</b> \"{escape(task['description'])}\"",
            parse_mode="HTML",
        )

    elif data == "skip_leave":
        await query.edit_message_text(
            "👍 <b>OK, leaving it as is.</b>", parse_mode="HTML",
        )
