# Telegram Task & Reminder Bot

A personal productivity bot for Telegram that lets you manage tasks through natural language. Type things like *"Buy groceries tomorrow at 3pm"* or *"Gym every Monday and Wednesday"* and the bot handles the rest.

---

## Features

- **Natural language task management** — add, complete, edit, delete, and reschedule tasks by typing naturally. The bot uses an LLM to parse your intent, including compound multi-action messages.
- **Recurring tasks** — daily, weekly, biweekly, monthly, specific days, or custom intervals. Auto-generated each morning.
- **Labels** — built-in categories (Home, Work, Health, Learning, Errands) with auto-assignment, plus custom labels with any emoji.
- **Morning prompt (7 AM)** — greeting, fun fact, week preview, existing tasks, overdue warnings, and a morning routine checklist with interactive toggle buttons.
- **Evening review (9 PM)** — walk through each task with Done / Not Done buttons. Carry over, reschedule, or drop incomplete tasks.
- **Reminders** — nudges at ~24h and ~2h before deadlines, with snooze options.
- **Weekly summary** — completion stats every Sunday evening.
- **Undo, history, backup, bulk clear** — and more via 20+ commands or plain text.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Telegram | [python-telegram-bot](https://python-telegram-bot.org/) v22+ |
| NLP | Any OpenAI-compatible LLM API (OpenAI, Anthropic, Groq, Gemini, Ollama) |
| Database | SQLite (WAL mode) |
| Validation | Pydantic v2 |
| Deployment | Docker on [Fly.io](https://fly.io/) with persistent volume |

---

## Project Structure

```
bot/
├── main.py          # Entry point, command registration
├── config.py        # Environment variables, timezone, schedule settings
├── database.py      # SQLite operations (tasks, labels, routines, recurrence)
├── handlers.py      # Command handlers and natural language routing
├── callbacks.py     # Inline keyboard handlers, morning/review jobs
├── formatting.py    # HTML message templates
├── nlp.py           # LLM integration for intent classification
├── models.py        # Pydantic models
├── scheduler.py     # Job scheduling (prompts, reminders, backups)
└── utils.py         # Shared utilities
```

---

## Setup

### 1. Install

```bash
git clone https://github.com/orionEsu/reminder-bot.git
cd reminder-bot
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env`:

```env
TELEGRAM_BOT_TOKEN=your-bot-token
AUTHORIZED_USER_ID=your-telegram-user-id
LLM_BASE_URL=https://api.groq.com/openai/v1
LLM_API_KEY=your-api-key
LLM_MODEL=llama-3.3-70b-versatile
```

Get your bot token from [@BotFather](https://t.me/BotFather) and your user ID from [@userinfobot](https://t.me/userinfobot).

### 3. Run

```bash
python -m bot.main
```

### 4. Run tests

```bash
pip install pytest pytest-asyncio
python -m pytest tests/ -v
```

Tests use a temporary database — no `.env` or API keys needed.

### Deploy to Fly.io

```bash
fly launch
fly secrets set TELEGRAM_BOT_TOKEN=... AUTHORIZED_USER_ID=... LLM_BASE_URL=... LLM_API_KEY=... LLM_MODEL=...
fly deploy
```

---

## Commands

| Command | Description |
|---------|-------------|
| `/tasks` | Today's tasks |
| `/upcoming` | All upcoming tasks |
| `/add` | Add a task |
| `/done` / `/delete` / `/edit` | Manage tasks by number |
| `/routine` | Morning routine (add/remove/list) |
| `/labels` / `/newlabel` / `/filter` | Label management |
| `/review` | Trigger daily review |
| `/status` / `/history` | Overview and completed tasks |
| `/stoprecur` | Stop a recurring task |
| `/undo` / `/backup` / `/clear` | Undo, backup, bulk delete |

Or skip commands and just type naturally.
