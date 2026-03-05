import json
import logging
from datetime import datetime

from bot.config import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL, TIMEZONE
from bot.models import ParsedTask

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are the brain of a task/reminder Telegram bot. The user sends natural language messages. Classify the intent and extract structured data.

Respond with ONLY a JSON object (no markdown, no extra text).

INTENTS:

1. ADD A TASK:
{{
  "intent": "add_task",
  "description": "clean task description",
  "due_date": "YYYY-MM-DD",
  "due_time": "HH:MM" or null,
  "confidence": 0.0 to 1.0,
  "recurrence_rule": null or "daily" or "weekly:DAY" or "biweekly:DAY" or "monthly:DD" or "specific:day1,day2",
  "labels": [],
  "notes": "additional context or details" or null
}}
If the user provides extra context beyond the task title, put it in "notes". E.g. "Buy groceries tomorrow - need milk, eggs, and bread from ShopRite" -> description="Buy groceries", notes="Need milk, eggs, and bread from ShopRite"

2. QUERY TASKS:
{{ "intent": "query", "query_type": "today" or "upcoming" or "review" or "filter" or "overdue" or "status" or "history", "filter_label": "name", "history_period": "today" or "week" or "month" or "all" }}
Examples: "show my tasks", "what's on today", "upcoming tasks", "show work tasks", "start review"
"show overdue tasks" / "what did I miss" -> query_type: "overdue"
"what's my status" / "overview" / "how am I doing" -> query_type: "status"
"what did I complete" / "show completed tasks" / "done tasks this week" -> query_type: "history" (extract period if mentioned, default "week")

3. MARK TASK DONE:
{{ "intent": "done", "task_id": 5, "task_description": null }}
Examples: "mark task 5 as done", "I finished task 5", "done with #5", "completed task 5", "finished the groceries task"
Use task_id if the user gives a number. Use task_description (a keyword from the task name) if they refer to a task by name.

4. DELETE TASK:
{{ "intent": "delete", "task_id": 3, "task_description": null }}
Examples: "delete task 3", "remove task #3", "cancel task 3", "delete the groceries task", "remove mechanic task"
Use task_id if the user gives a number. Use task_description (a keyword from the task name) if they refer to a task by name.

5. LIST LABELS:
{{ "intent": "list_labels" }}
Examples: "show labels", "what labels do I have", "existing labels", "my labels"

6. ADD LABEL:
{{ "intent": "add_label", "emoji": "⛪", "name": "Church" }}
Examples: "add new label church", "create label called Church with ⛪", "new label - church"
If no emoji given, pick an appropriate one.

7. EDIT LABEL:
{{ "intent": "edit_label", "old_name": "Church", "new_emoji": "🙏", "new_name": "Faith" }}
Examples: "rename church label to Faith", "change church emoji to 🙏"

8. DELETE LABEL:
{{ "intent": "delete_label", "name": "Church" }}
Examples: "delete church label", "remove the church label"

9. STOP RECURRENCE:
{{ "intent": "stop_recur", "task_id": 7, "task_description": null }}
Examples: "stop recurring task 7", "cancel recurrence for #7"

10. VIEW TASK DETAILS:
{{ "intent": "view_task", "task_id": 5, "task_description": null }}
Examples: "show task 5", "details for task #5", "what's task 5 about", "info on task 5", "tell me about the groceries task"

11. ADD/UPDATE NOTES:
{{ "intent": "update_notes", "task_id": 5, "task_description": null, "notes": "the new notes text" }}
Examples: "add note to task 5: bring the blue folder", "update notes for task 5 - call John first"

12. ASSIGN LABEL TO TASK:
{{ "intent": "assign_label", "task_id": 3, "task_description": null, "label_name": "Social" }}
Examples: "add social label to task 3", "tag task #3 as Work", "label task 5 as Home", "attach errands label to task 2"

13. REMOVE LABEL FROM TASK:
{{ "intent": "remove_label", "task_id": 3, "task_description": null, "label_name": "Social" }}
Examples: "remove social label from task 3", "untag task 3 from Work"

14. EDIT TASK:
{{ "intent": "edit_task", "task_id": 5, "task_description": null, "new_description": null, "new_date": "YYYY-MM-DD" or null, "new_time": "HH:MM" or null, "reason": "move" or "rename" or "edit" }}
Examples: "move task 5 to Friday", "rename task 3 to Buy milk", "change task 5 time to 3pm", "reschedule task 2 to next Monday", "carry over task 2 to tomorrow", "push task 4 to next week", "postpone task 3", "shift task 1 to evening", "bump task 6 to Monday", "move the groceries task to Friday"
At least one of new_description, new_date, new_time must be non-null. Use the same date/time rules as add_task.
"reason" reflects the user's intent: "move" for carry over/reschedule/push/postpone/shift/bump/defer/delay/move, "rename" for changing description/rename/reword, "edit" for everything else.

15. UNDO:
{{ "intent": "undo" }}
Examples: "undo", "undo that", "revert", "take that back"

16. BACKUP:
{{ "intent": "backup" }}
Examples: "backup my data", "send me the database", "export my tasks"

17. COMPOUND ACTIONS (multiple things in one message):
{{ "intent": "compound", "actions": [action1, action2, ...] }}
When the user asks to do multiple things at once — whether on separate lines, separated by "and", or in any combined form — return a compound intent with an array of individual action objects. Each action must be a complete intent object.
Examples:
- "create social label and add it to task 3" -> compound with add_label + assign_label
- "delete task 1\nadd go to the mechanic tomorrow" -> compound with delete + add_task
- "mark task 2 done and move task 3 to Friday" -> compound with done + edit_task
- "buy milk tomorrow and call dentist on Monday" -> compound with two add_task actions

18. HELP:
{{ "intent": "help" }}
Examples: "help", "what can you do", "how do I use this", "commands"

19. UNKNOWN:
{{ "intent": "unknown" }}

IMPORTANT: Always respond with a SINGLE valid JSON object. Never output multiple JSON objects or extra text.

RULES:
- Current date/time: {now}. Timezone: WAT (UTC+1).
- "Tomorrow" = next day. "Today" = current date.
- "Morning" = 09:00, "afternoon" = 14:00, "evening" = 19:00.
- If no date mentioned for a task, assume today.
- If no time mentioned, due_time = null.
- Recurrence: "every day"->"daily", "every Monday"->"weekly:monday", "every other Friday"->"biweekly:friday", "1st of every month"->"monthly:1", "Mon, Wed, Fri"->"specific:mon,wed,fri". For recurring, due_date = next occurrence.
- Label inference: cleaning/cooking/laundry->"Home", meeting/deadline/email->"Work", gym/exercise/run->"Health", study/read/course->"Learning", buy/shop/errand->"Errands". Empty list if unsure.
- Available labels: {labels}
- confidence: 1.0 = very certain, lower if ambiguous.
- Task references: When the user refers to a task by number, use "task_id". When they refer by name/description (e.g. "the groceries task", "mechanic task"), use "task_description" with a keyword. Only one of task_id or task_description should be non-null.
- Multi-line or multi-action messages: If the message contains multiple commands (on separate lines or joined by "and"/"then"), ALWAYS use the compound intent to wrap them all."""

MORNING_SYSTEM_PROMPT = """You are a task extraction assistant. The user is listing tasks for today in response to a morning planning prompt.

Extract ALL tasks from the message. Return ONLY a JSON array (no markdown):
[
  {{
    "description": "task description",
    "due_time": "HH:MM" or null,
    "recurrence_rule": null or "daily"/"weekly:DAY"/etc.,
    "labels": [] or subset of ["Home", "Work", "Health", "Learning", "Errands"]
  }}
]

Rules:
- Current date: {today}. All tasks default to today.
- Extract multiple tasks if listed (commas, newlines, numbers).
- Same label inference rules as before.
- If the message says "done", "that's all", "nothing", "nah", "no", return an empty array [].
- Keep descriptions concise and clean."""

FUN_FACT_PROMPT = "Give me one short, interesting fun fact (1-2 sentences). Be diverse in topics — science, history, nature, space, animals, technology, food, culture, etc. Just the fact, no preamble."

import asyncio
import time as _time

_is_anthropic = "anthropic.com" in LLM_BASE_URL
_client = None
_last_call_ts = 0.0
_MIN_CALL_INTERVAL = 1.0  # seconds between LLM calls


def _get_client():
    global _client
    if _client is None:
        if _is_anthropic:
            import anthropic
            _client = anthropic.Anthropic(api_key=LLM_API_KEY)
        else:
            from openai import OpenAI
            _client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)
    return _client


def _call_llm_sync(system: str, user_text: str, max_tokens: int = 256) -> str:
    client = _get_client()
    if _is_anthropic:
        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user_text}],
        )
        return response.content[0].text
    else:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_text},
            ],
            temperature=0,
        )
        return response.choices[0].message.content


async def _call_llm(system: str, user_text: str, max_tokens: int = 256) -> str:
    """Run the blocking LLM call in a thread to avoid freezing the event loop."""
    global _last_call_ts
    now = _time.monotonic()
    wait = _MIN_CALL_INTERVAL - (now - _last_call_ts)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call_ts = _time.monotonic()
    return await asyncio.to_thread(_call_llm_sync, system, user_text, max_tokens)


def _strip_fences(content: str) -> str:
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3].strip()
    return content


def _extract_json(content: str) -> dict | list:
    """Extract the first valid JSON object/array from a string, even if there's extra text."""
    content = _strip_fences(content)
    # Try direct parse first
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    # Find the first { or [ and try json.loads from that position
    # Use json.loads directly — it handles strings with braces correctly
    for i, ch in enumerate(content):
        if ch in ('{', '['):
            # Try parsing from this position, shrinking from the end
            # json.loads will correctly handle braces inside string literals
            for j in range(len(content), i, -1):
                try:
                    return json.loads(content[i:j])
                except json.JSONDecodeError:
                    continue
            break
    raise json.JSONDecodeError("No valid JSON found", content, 0)


async def parse_task_message(user_text: str, available_labels: list[str] | None = None) -> ParsedTask | dict | None:
    """Returns ParsedTask for add_task, dict for other intents, None on failure."""
    now = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    # Sanitize label names to prevent prompt injection
    if available_labels:
        safe_labels = [name.replace("{", "").replace("}", "")[:50] for name in available_labels]
        labels_str = ", ".join(safe_labels)
    else:
        labels_str = "Home, Work, Health, Learning, Errands"

    try:
        content = await _call_llm(SYSTEM_PROMPT.format(now=now, labels=labels_str), user_text, max_tokens=512)
        data = _extract_json(content)
        intent = data.get("intent", "unknown")

        if intent == "add_task":
            due_date = data.get("due_date", "")
            due_time = data.get("due_time")
            # Validate date format
            try:
                datetime.strptime(due_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                logger.warning("Invalid due_date from LLM: %s", due_date)
                return None
            # Validate time format if present
            if due_time:
                try:
                    datetime.strptime(due_time, "%H:%M")
                except (ValueError, TypeError):
                    due_time = None

            parsed = ParsedTask(
                description=data.get("description", "").strip(),
                due_date=due_date,
                due_time=due_time,
                confidence=data.get("confidence", 1.0),
                recurrence_rule=data.get("recurrence_rule"),
                label_names=data.get("labels", []),
                notes=data.get("notes"),
            )
            if not parsed.description:
                return None
            if parsed.confidence >= 0.3:
                return parsed
            return None
        elif intent == "unknown":
            return None
        else:
            # Return the full dict for all other intents (query, done, delete, labels, help, etc.)
            return data

    except Exception as e:
        logger.error("NLP parsing failed: %s", e)
        return None


async def parse_morning_tasks(user_text: str) -> list[ParsedTask]:
    """Parse multiple tasks from a morning prompt response."""
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")

    try:
        content = await _call_llm(MORNING_SYSTEM_PROMPT.format(today=today), user_text, max_tokens=512)
        items = _extract_json(content)

        if not isinstance(items, list):
            return []

        tasks = []
        for item in items:
            tasks.append(ParsedTask(
                description=item["description"],
                due_date=today,
                due_time=item.get("due_time"),
                confidence=1.0,
                recurrence_rule=item.get("recurrence_rule"),
                label_names=item.get("labels", []),
            ))
        return tasks

    except Exception as e:
        logger.error("Morning tasks parsing failed: %s", e)
        return []


async def generate_fun_fact() -> str:
    """Generate a fun fact via LLM."""
    try:
        return (await _call_llm("You provide fun facts.", FUN_FACT_PROMPT, max_tokens=100)).strip()
    except Exception as e:
        logger.error("Fun fact generation failed: %s", e)
        return "Honey never spoils — archaeologists have found 3000-year-old honey in Egyptian tombs that was still perfectly edible!"
