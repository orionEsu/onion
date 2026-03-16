import logging
import shutil
import sqlite3
from datetime import datetime, time, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

from bot.config import (
    TIMEZONE, DAILY_REVIEW_HOUR, DAILY_REVIEW_MINUTE,
    MORNING_PROMPT_HOUR, MORNING_PROMPT_MINUTE, AUTHORIZED_USER_ID,
    WEEKLY_SUMMARY_DAY, WEEKLY_SUMMARY_HOUR, WEEKLY_SUMMARY_MINUTE,
    DAILY_BACKUP_HOUR, DAILY_BACKUP_MINUTE, DB_PATH,
    REMINDER_OFFSETS,
)
from bot import database as db
from bot import formatting as fmt
from bot.callbacks import send_daily_review, send_morning_prompt

logger = logging.getLogger(__name__)


def schedule_jobs(application: Application) -> None:
    job_queue = application.job_queue

    # Morning prompt
    morning_time = time(
        hour=MORNING_PROMPT_HOUR, minute=MORNING_PROMPT_MINUTE, tzinfo=TIMEZONE,
    )
    job_queue.run_daily(send_morning_prompt, time=morning_time, name="morning_prompt")

    # Daily review
    review_time = time(
        hour=DAILY_REVIEW_HOUR, minute=DAILY_REVIEW_MINUTE, tzinfo=TIMEZONE,
    )
    job_queue.run_daily(send_daily_review, time=review_time, name="daily_review")

    # Reminder check every 5 minutes
    job_queue.run_repeating(check_reminders, interval=300, first=10, name="reminder_check")

    # Morning prompt auto-timeout (2 hours after morning prompt)
    timeout_hour = (MORNING_PROMPT_HOUR + 2) % 24
    timeout_time = time(
        hour=timeout_hour, minute=MORNING_PROMPT_MINUTE, tzinfo=TIMEZONE,
    )
    job_queue.run_daily(
        end_morning_prompt_timeout, time=timeout_time, name="morning_timeout",
    )

    # Weekly summary
    summary_time = time(
        hour=WEEKLY_SUMMARY_HOUR, minute=WEEKLY_SUMMARY_MINUTE, tzinfo=TIMEZONE,
    )
    job_queue.run_daily(
        send_weekly_summary, time=summary_time,
        days=(WEEKLY_SUMMARY_DAY,), name="weekly_summary",
    )

    # Daily backup at midnight
    backup_time = time(
        hour=DAILY_BACKUP_HOUR, minute=DAILY_BACKUP_MINUTE, tzinfo=TIMEZONE,
    )
    job_queue.run_daily(daily_backup, time=backup_time, name="daily_backup")

    logger.info(
        "Scheduled: morning prompt at %02d:%02d, daily review at %02d:%02d, "
        "reminders every 5 min, weekly summary, daily backup",
        MORNING_PROMPT_HOUR, MORNING_PROMPT_MINUTE,
        DAILY_REVIEW_HOUR, DAILY_REVIEW_MINUTE,
    )


async def check_reminders(context) -> None:
    try:
        await _check_reminders_inner(context)
    except Exception:
        logger.exception("check_reminders job failed")


def _format_time_remaining(total_minutes: int) -> str:
    """Convert minutes into a human-readable time-remaining label."""
    if total_minutes >= 1440:
        hours = total_minutes // 60
        return f"{hours} hours"
    if total_minutes >= 60:
        hours = total_minutes // 60
        mins = total_minutes % 60
        return f"{hours}h {mins}m" if mins else f"{hours} hours"
    return f"{total_minutes} minutes"


async def _check_reminders_inner(context) -> None:
    now = datetime.now(TIMEZONE)

    # Phase 1: Global offsets
    for offset in REMINDER_OFFSETS:
        tasks = db.get_tasks_needing_reminder(offset, now)
        if not tasks:
            continue
        labels_map = db.get_labels_for_tasks([t["id"] for t in tasks])
        for task in tasks:
            due_dt = datetime.strptime(
                f"{task['due_date']} {task['due_time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=TIMEZONE)
            diff = due_dt - now
            total_minutes = int(diff.total_seconds() / 60)
            label = _format_time_remaining(total_minutes)

            labels = labels_map.get(task["id"], [])
            tid = task["id"]
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("😴 1h", callback_data=f"snooze_1h_{tid}"),
                InlineKeyboardButton("😴 3h", callback_data=f"snooze_3h_{tid}"),
                InlineKeyboardButton("📅 Tomorrow", callback_data=f"snooze_tomorrow_{tid}"),
            ]])
            await context.bot.send_message(
                chat_id=AUTHORIZED_USER_ID,
                text=fmt.format_reminder(task, label, labels),
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            db.mark_reminder_sent(task["id"], offset)
            # Store last reminded task so "already done" / "I did that" can resolve it
            context.application.bot_data["last_reminded_task_id"] = task["id"]

    # Phase 2: Custom per-task reminders
    await _check_custom_reminders(context, now)


async def _check_custom_reminders(context, now: datetime) -> None:
    """Check and fire all pending custom reminders (absolute, offset, repeating)."""

    def _snooze_keyboard(tid):
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("😴 1h", callback_data=f"snooze_1h_{tid}"),
            InlineKeyboardButton("😴 3h", callback_data=f"snooze_3h_{tid}"),
            InlineKeyboardButton("📅 Tomorrow", callback_data=f"snooze_tomorrow_{tid}"),
        ]])

    # Absolute reminders
    for row in db.get_pending_absolute_reminders(now):
        tid = row["task_id"]
        labels = db.get_labels_for_task(tid)
        task = db.get_task(tid)
        if not task:
            continue
        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=fmt.format_custom_reminder_notification(task, "absolute", labels),
            parse_mode="HTML",
            reply_markup=_snooze_keyboard(tid),
        )
        db.mark_custom_reminder_fired(row["id"])
        context.application.bot_data["last_reminded_task_id"] = tid

    # Offset reminders
    for row in db.get_pending_offset_reminders(now):
        tid = row["task_id"]
        labels = db.get_labels_for_task(tid)
        task = db.get_task(tid)
        if not task:
            continue
        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=fmt.format_custom_reminder_notification(task, "offset", labels),
            parse_mode="HTML",
            reply_markup=_snooze_keyboard(tid),
        )
        db.mark_custom_reminder_fired(row["id"])
        context.application.bot_data["last_reminded_task_id"] = tid

    # Repeating reminders
    for row in db.get_pending_repeating_reminders(now):
        tid = row["task_id"]
        labels = db.get_labels_for_task(tid)
        task = db.get_task(tid)
        if not task:
            continue
        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=fmt.format_custom_reminder_notification(task, "repeating", labels),
            parse_mode="HTML",
            reply_markup=_snooze_keyboard(tid),
        )
        db.mark_custom_reminder_fired(row["id"], fired_at=now.strftime("%Y-%m-%d %H:%M"))
        context.application.bot_data["last_reminded_task_id"] = tid


async def end_morning_prompt_timeout(context) -> None:
    """Auto-end morning prompt after 2 hours if still active."""
    try:
        if not context.application.bot_data.get("morning_prompt_active"):
            return

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
        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=msg,
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("end_morning_prompt_timeout job failed")


async def send_weekly_summary(context) -> None:
    """Send weekly summary on configured day."""
    try:
        today = datetime.now(TIMEZONE)
        start_of_week = (today - timedelta(days=6)).strftime("%Y-%m-%d")
        end_of_week = today.strftime("%Y-%m-%d")

        stats = db.get_weekly_stats(start_of_week, end_of_week)
        msg = fmt.format_weekly_summary(stats)
        await context.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text=msg,
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("send_weekly_summary job failed")


async def daily_backup(context) -> None:
    """Create a local backup of the database file at midnight and send it via Telegram."""
    import os
    import sys
    import tempfile
    backup_path = str(DB_PATH) + ".bak"
    try:
        src = sqlite3.connect(str(DB_PATH))
        dst = sqlite3.connect(backup_path)
        src.backup(dst)
        src.close()
        dst.close()
        # Restrict permissions on Linux/macOS (owner read/write only)
        if sys.platform != "win32":
            os.chmod(backup_path, 0o600)
        logger.info("Daily backup created: %s", backup_path)

        # Send backup to user via Telegram
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        filename = f"tasks_backup_{today}.db"
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp_path = tmp.name
        tmp.close()
        try:
            src2 = sqlite3.connect(str(DB_PATH))
            dst2 = sqlite3.connect(tmp_path)
            src2.backup(dst2)
            src2.close()
            dst2.close()
            with open(tmp_path, "rb") as f:
                await context.bot.send_document(
                    chat_id=AUTHORIZED_USER_ID,
                    document=f,
                    filename=filename,
                    caption=f"💾 <b>Daily backup</b> — {today}",
                    parse_mode="HTML",
                )
            logger.info("Daily backup sent to user via Telegram")
        except Exception as e:
            logger.error("Failed to send backup via Telegram: %s", e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    except Exception as e:
        logger.error("Daily backup failed: %s", e)
