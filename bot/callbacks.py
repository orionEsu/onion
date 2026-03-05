from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.config import AUTHORIZED_USER_ID, TIMEZONE
from bot import database as db
from bot import formatting as fmt
from bot import nlp
from bot.utils import store_undo, task_to_dict


async def send_morning_prompt(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled at 7 AM. Sends varied greeting + fun fact + existing tasks."""
    # Auto-generate today's recurring task instances
    db.generate_recurring_for_today()

    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    existing = db.get_tasks_for_date(today)

    fun_fact = await nlp.generate_fun_fact()
    msg = fmt.format_morning_prompt(fun_fact)

    if existing:
        labels_map = db.get_labels_for_tasks([t["id"] for t in existing])
        msg += "\n\n" + fmt.format_task_list("📋 <b>Already on your plate</b>", existing, labels_map)

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


async def send_daily_review(context: ContextTypes.DEFAULT_TYPE):
    """Scheduled at 9 PM. Sends review with inline buttons."""
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
        new_dt = due_dt + timedelta(hours=hours)
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
        tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime("%Y-%m-%d")
        db.carry_over_task(task_id, tomorrow, None)
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

    # ── Snooze callbacks ──

    elif data.startswith("snooze_"):
        await _handle_snooze(query, data)

    # ── Morning prompt done ──

    elif data == "morning_done":
        context.application.bot_data["morning_prompt_active"] = False
        added_ids = context.application.bot_data.pop("morning_prompt_tasks", [])

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        all_tasks = db.get_tasks_for_date(today)
        added_tasks = [db.get_task(tid) for tid in added_ids if db.get_task(tid)]

        msg = fmt.format_morning_summary(added_tasks, all_tasks)
        await query.edit_message_text(msg, parse_mode="HTML")
