import os
from pathlib import Path
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

load_dotenv()

BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
AUTHORIZED_USER_ID: int = int(os.environ["AUTHORIZED_USER_ID"])

# LLM settings (OpenAI-compatible API)
LLM_BASE_URL: str = os.environ["LLM_BASE_URL"]
LLM_API_KEY: str = os.environ["LLM_API_KEY"]
LLM_MODEL: str = os.environ["LLM_MODEL"]

# WAT = UTC+1
TIMEZONE = ZoneInfo("Africa/Lagos")

# Morning prompt
MORNING_PROMPT_HOUR: int = int(os.getenv("MORNING_PROMPT_HOUR", "7"))
MORNING_PROMPT_MINUTE: int = int(os.getenv("MORNING_PROMPT_MINUTE", "0"))

# Daily review
DAILY_REVIEW_HOUR: int = int(os.getenv("DAILY_REVIEW_HOUR", "21"))
DAILY_REVIEW_MINUTE: int = int(os.getenv("DAILY_REVIEW_MINUTE", "0"))

# Weekly summary
WEEKLY_SUMMARY_DAY: int = int(os.getenv("WEEKLY_SUMMARY_DAY", "6"))  # 0=Mon, 6=Sun
WEEKLY_SUMMARY_HOUR: int = int(os.getenv("WEEKLY_SUMMARY_HOUR", "20"))
WEEKLY_SUMMARY_MINUTE: int = int(os.getenv("WEEKLY_SUMMARY_MINUTE", "0"))

# Daily backup
DAILY_BACKUP_HOUR: int = int(os.getenv("DAILY_BACKUP_HOUR", "0"))
DAILY_BACKUP_MINUTE: int = int(os.getenv("DAILY_BACKUP_MINUTE", "0"))

# Reminder offsets in minutes before due time (comma-separated)
# Default: 24 hours, 2 hours, 30 minutes
_raw_offsets = os.getenv("REMINDER_OFFSETS", "1440,120,30")
REMINDER_OFFSETS: list[int] = sorted(
    [int(x.strip()) for x in _raw_offsets.split(",") if x.strip().isdigit()],
    reverse=True,
)

# Undo expiry
UNDO_EXPIRY_SECONDS: int = 300  # 5 minutes

DB_PATH: Path = Path(os.getenv("DB_PATH", str(Path(__file__).resolve().parent.parent / "tasks.db")))
