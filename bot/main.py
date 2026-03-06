import logging

from telegram import BotCommand
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from bot.config import BOT_TOKEN, AUTHORIZED_USER_ID
from bot.database import init_db
from bot.handlers import (
    start_command,
    help_command,
    add_command,
    tasks_command,
    upcoming_command,
    done_command,
    delete_command,
    review_command,
    stop_recurring_command,
    labels_command,
    newlabel_command,
    editlabel_command,
    deletelabel_command,
    filter_command,
    edit_command,
    undo_command,
    status_command,
    history_command,
    backup_command,
    clear_command,
    routine_command,
    handle_natural_language,
)
from bot.callbacks import handle_callback
from bot.scheduler import schedule_jobs

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_COMMANDS = [
    BotCommand("tasks", "📋 Today's tasks"),
    BotCommand("upcoming", "📅 All upcoming tasks"),
    BotCommand("add", "➕ Add a task"),
    BotCommand("done", "✅ Mark task done"),
    BotCommand("delete", "🗑️ Delete a task"),
    BotCommand("edit", "✏️ Edit a task"),
    BotCommand("undo", "↩️ Undo last action"),
    BotCommand("status", "📊 Status overview"),
    BotCommand("history", "📜 Completed tasks"),
    BotCommand("review", "🌙 Daily review"),
    BotCommand("labels", "🏷️ List labels"),
    BotCommand("newlabel", "🆕 Create a label"),
    BotCommand("filter", "🔍 Filter by label"),
    BotCommand("stoprecur", "🛑 Stop recurring task"),
    BotCommand("backup", "💾 Backup database"),
    BotCommand("routine", "🌅 Morning routine"),
    BotCommand("clear", "🧹 Clear tasks"),
    BotCommand("help", "📖 Show help"),
]


async def post_init(application: Application) -> None:
    """Set the bot's command menu and initialize state after startup."""
    await application.bot.set_my_commands(BOT_COMMANDS)
    logger.info("Bot menu commands set (%d commands)", len(BOT_COMMANDS))

    # Initialize in-memory state (lost on restart)
    application.bot_data.setdefault("morning_prompt_active", False)
    application.bot_data.setdefault("morning_prompt_tasks", [])
    application.bot_data.setdefault("last_undo", None)

    # Notify user of restart
    try:
        await application.bot.send_message(
            chat_id=AUTHORIZED_USER_ID,
            text="🔄 <b>Bot restarted.</b> All scheduled jobs active.",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("Could not send startup notification: %s", e)


async def error_handler(update: object, context) -> None:
    logger.error("Exception while handling update:", exc_info=context.error)
    if update and hasattr(update, "effective_message") and update.effective_message:
        await update.effective_message.reply_text(
            "❌ Something went wrong. Please try again.", parse_mode="HTML",
        )


def main() -> None:
    init_db()

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Task commands
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("add", add_command))
    app.add_handler(CommandHandler("tasks", tasks_command))
    app.add_handler(CommandHandler("upcoming", upcoming_command))
    app.add_handler(CommandHandler("done", done_command))
    app.add_handler(CommandHandler("delete", delete_command))
    app.add_handler(CommandHandler("review", review_command))
    app.add_handler(CommandHandler("stoprecur", stop_recurring_command))
    app.add_handler(CommandHandler("edit", edit_command))
    app.add_handler(CommandHandler("undo", undo_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("backup", backup_command))
    app.add_handler(CommandHandler("clear", clear_command))
    app.add_handler(CommandHandler("routine", routine_command))

    # Label commands
    app.add_handler(CommandHandler("labels", labels_command))
    app.add_handler(CommandHandler("newlabel", newlabel_command))
    app.add_handler(CommandHandler("editlabel", editlabel_command))
    app.add_handler(CommandHandler("deletelabel", deletelabel_command))
    app.add_handler(CommandHandler("filter", filter_command))

    # Inline keyboard callbacks
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Natural language catch-all (must be last)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_natural_language))

    # Error handler
    app.add_error_handler(error_handler)

    # Scheduled jobs
    schedule_jobs(app)

    logger.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
