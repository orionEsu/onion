"""Tests for the Daily Morning Routine & Smart Briefing feature."""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from html import escape

from bot import database as db
from bot import formatting as fmt


# ── Database CRUD ─────────────────────────────────────────────────


class TestRoutineDatabase:
    def test_add_routine_item_basic(self):
        rid = db.add_routine_item("Drink water")
        assert rid is not None
        item = db.get_routine_item(rid)
        assert item["description"] == "Drink water"
        assert item["target_time"] is None

    def test_add_routine_item_with_time(self):
        rid = db.add_routine_item("Exercise", "07:30")
        item = db.get_routine_item(rid)
        assert item["target_time"] == "07:30"

    def test_sort_order_auto_increments(self):
        r1 = db.add_routine_item("First")
        r2 = db.add_routine_item("Second")
        r3 = db.add_routine_item("Third")
        items = db.get_all_routine_items()
        assert len(items) >= 3
        orders = [i["sort_order"] for i in items]
        assert orders == sorted(orders), "Items should be in ascending sort_order"

    def test_get_all_routine_items_empty(self):
        items = db.get_all_routine_items()
        assert items == []

    def test_get_all_routine_items_ordered(self):
        db.add_routine_item("A")
        db.add_routine_item("B")
        items = db.get_all_routine_items()
        assert len(items) == 2
        assert items[0]["description"] == "A"
        assert items[1]["description"] == "B"

    def test_get_routine_item_not_found(self):
        assert db.get_routine_item(9999) is None

    def test_get_routine_item_by_description_match(self):
        db.add_routine_item("Morning devotion")
        item = db.get_routine_item_by_description("devotion")
        assert item is not None
        assert "devotion" in item["description"].lower()

    def test_get_routine_item_by_description_no_match(self):
        db.add_routine_item("Drink water")
        assert db.get_routine_item_by_description("zzzzzzz") is None

    def test_get_routine_item_by_description_case_insensitive(self):
        db.add_routine_item("Exercise")
        item = db.get_routine_item_by_description("EXERCISE")
        assert item is not None

    def test_delete_routine_item(self):
        rid = db.add_routine_item("To delete")
        assert db.delete_routine_item(rid) is True
        assert db.get_routine_item(rid) is None

    def test_delete_routine_item_not_found(self):
        assert db.delete_routine_item(9999) is False

    def test_delete_cascades_completions(self):
        rid = db.add_routine_item("Temporary")
        db.complete_routine_item(rid, "2026-03-06")
        assert rid in db.get_routine_completions_for_date("2026-03-06")
        db.delete_routine_item(rid)
        assert rid not in db.get_routine_completions_for_date("2026-03-06")


class TestRoutineCompletions:
    def test_complete_and_get(self):
        rid = db.add_routine_item("Water")
        db.complete_routine_item(rid, "2026-03-06")
        comps = db.get_routine_completions_for_date("2026-03-06")
        assert rid in comps

    def test_complete_idempotent(self):
        rid = db.add_routine_item("Water")
        db.complete_routine_item(rid, "2026-03-06")
        db.complete_routine_item(rid, "2026-03-06")  # no error
        comps = db.get_routine_completions_for_date("2026-03-06")
        assert rid in comps

    def test_uncomplete(self):
        rid = db.add_routine_item("Water")
        db.complete_routine_item(rid, "2026-03-06")
        db.uncomplete_routine_item(rid, "2026-03-06")
        comps = db.get_routine_completions_for_date("2026-03-06")
        assert rid not in comps

    def test_uncomplete_nonexistent_no_error(self):
        db.uncomplete_routine_item(9999, "2026-03-06")  # should not raise

    def test_completions_are_date_scoped(self):
        rid = db.add_routine_item("Water")
        db.complete_routine_item(rid, "2026-03-06")
        assert rid in db.get_routine_completions_for_date("2026-03-06")
        assert rid not in db.get_routine_completions_for_date("2026-03-07")

    def test_is_routine_all_complete_true(self):
        r1 = db.add_routine_item("A")
        r2 = db.add_routine_item("B")
        db.complete_routine_item(r1, "2026-03-06")
        db.complete_routine_item(r2, "2026-03-06")
        assert db.is_routine_all_complete("2026-03-06") is True

    def test_is_routine_all_complete_false(self):
        r1 = db.add_routine_item("A")
        r2 = db.add_routine_item("B")
        db.complete_routine_item(r1, "2026-03-06")
        assert db.is_routine_all_complete("2026-03-06") is False

    def test_is_routine_all_complete_no_items(self):
        assert db.is_routine_all_complete("2026-03-06") is False

    def test_is_routine_all_complete_different_day(self):
        r1 = db.add_routine_item("A")
        db.complete_routine_item(r1, "2026-03-05")
        assert db.is_routine_all_complete("2026-03-06") is False


class TestWeekTaskCounts:
    def test_counts_grouped_by_date(self):
        db.add_task("T1", "2026-03-06", None)
        db.add_task("T2", "2026-03-06", "10:00")
        db.add_task("T3", "2026-03-08", None)
        counts = db.get_week_task_counts("2026-03-06")
        assert counts.get("2026-03-06") == 2
        assert counts.get("2026-03-08") == 1

    def test_counts_excludes_done(self):
        tid = db.add_task("Done task", "2026-03-06", None)
        db.update_task_status(tid, "done")
        db.add_task("Pending", "2026-03-06", None)
        counts = db.get_week_task_counts("2026-03-06")
        assert counts.get("2026-03-06") == 1

    def test_counts_limited_to_7_days(self):
        db.add_task("In range", "2026-03-12", None)
        db.add_task("Out of range", "2026-03-15", None)
        counts = db.get_week_task_counts("2026-03-06")
        assert "2026-03-12" in counts
        assert "2026-03-15" not in counts

    def test_counts_empty(self):
        counts = db.get_week_task_counts("2026-03-06")
        assert counts == {}


# ── Formatting ────────────────────────────────────────────────────


class TestRoutineFormatting:
    def test_format_routine_list_empty(self):
        result = fmt.format_routine_list([])
        assert "No routine items" in result

    def test_format_routine_list_with_items(self):
        # Use dicts that mimic sqlite3.Row
        items = [
            {"id": 1, "description": "Drink water", "target_time": "07:00", "sort_order": 1},
            {"id": 2, "description": "Exercise", "target_time": None, "sort_order": 2},
        ]
        result = fmt.format_routine_list(items)
        assert "Drink water" in result
        assert "at 07:00" in result
        assert "Exercise" in result
        assert "1." in result
        assert "2." in result

    def test_format_routine_checklist_all_unchecked(self):
        items = [
            {"id": 1, "description": "Water", "target_time": "07:00"},
            {"id": 2, "description": "Exercise", "target_time": None},
        ]
        result = fmt.format_routine_checklist(items, set())
        assert result.count("\u2b1c") == 2  # ⬜
        assert "\u2705" not in result  # no ✅ in list lines (only in header... actually no ✅ at all)

    def test_format_routine_checklist_partial(self):
        items = [
            {"id": 1, "description": "Water", "target_time": None},
            {"id": 2, "description": "Exercise", "target_time": None},
        ]
        result = fmt.format_routine_checklist(items, {1})
        assert "\u2705" in result  # ✅ for item 1
        assert "\u2b1c" in result  # ⬜ for item 2

    def test_format_routine_checklist_all_checked(self):
        items = [
            {"id": 1, "description": "Water", "target_time": None},
            {"id": 2, "description": "Exercise", "target_time": None},
        ]
        result = fmt.format_routine_checklist(items, {1, 2})
        assert result.count("\u2705") == 2
        assert "\u2b1c" not in result

    def test_format_routine_checklist_empty(self):
        assert fmt.format_routine_checklist([], set()) == ""

    def test_format_routine_checklist_escapes_html(self):
        items = [{"id": 1, "description": "<script>alert(1)</script>", "target_time": None}]
        result = fmt.format_routine_checklist(items, set())
        assert "<script>" not in result
        assert "&lt;script&gt;" in result


class TestWeekPreviewFormatting:
    def test_format_week_preview_empty(self):
        assert fmt.format_week_preview({}) == ""

    def test_format_week_preview_with_counts(self):
        counts = {"2026-03-07": 2, "2026-03-09": 1}
        result = fmt.format_week_preview(counts)
        assert "This Week" in result
        assert "2" in result
        assert "1" in result

    def test_format_week_preview_sorted_by_date(self):
        counts = {"2026-03-09": 1, "2026-03-07": 3}
        result = fmt.format_week_preview(counts)
        lines = result.strip().split("\n")
        # The first data line (after header) should be the earlier date
        data_lines = [l for l in lines if "<b>" in l and "This Week" not in l]
        assert len(data_lines) == 2
        assert "3" in data_lines[0]  # March 7 has 3 tasks


class TestHelpIncludesRoutine:
    def test_help_mentions_routine(self):
        result = fmt.format_help()
        assert "/routine" in result
        assert "Morning routine" in result or "morning routine" in result.lower()


# ── Callbacks ─────────────────────────────────────────────────────


class TestRoutineCallbacks:
    @pytest.mark.asyncio
    async def test_routine_check_toggle_on(self, mock_update, mock_context):
        """Tapping an unchecked routine item should complete it."""
        from bot.callbacks import handle_callback

        rid = db.add_routine_item("Water")
        today = datetime.now().strftime("%Y-%m-%d")

        mock_update.effective_user.id = int(__import__("os").environ.get("AUTHORIZED_USER_ID", "0"))
        mock_update.callback_query.data = f"routine_check_{rid}"
        mock_context.application.bot_data = {}

        await handle_callback(mock_update, mock_context)

        comps = db.get_routine_completions_for_date(today)
        assert rid in comps

    @pytest.mark.asyncio
    async def test_routine_check_toggle_off(self, mock_update, mock_context):
        """Tapping a checked routine item should uncomplete it."""
        from bot.callbacks import handle_callback

        rid = db.add_routine_item("Water")
        today = datetime.now().strftime("%Y-%m-%d")
        db.complete_routine_item(rid, today)

        mock_update.effective_user.id = int(__import__("os").environ.get("AUTHORIZED_USER_ID", "0"))
        mock_update.callback_query.data = f"routine_check_{rid}"
        mock_context.application.bot_data = {}

        await handle_callback(mock_update, mock_context)

        comps = db.get_routine_completions_for_date(today)
        assert rid not in comps

    @pytest.mark.asyncio
    async def test_routine_congrats_sent_once(self, mock_update, mock_context):
        """Completing all items should send congrats exactly once."""
        from bot.callbacks import handle_callback

        rid = db.add_routine_item("Only item")
        today = datetime.now().strftime("%Y-%m-%d")

        mock_update.effective_user.id = int(__import__("os").environ.get("AUTHORIZED_USER_ID", "0"))
        mock_update.callback_query.data = f"routine_check_{rid}"
        mock_context.application.bot_data = {}

        await handle_callback(mock_update, mock_context)

        # Congrats should have been sent
        send_calls = mock_context.bot.send_message.call_args_list
        congrats_calls = [c for c in send_calls if "routine complete" in str(c).lower()]
        assert len(congrats_calls) == 1

        # Second toggle off and on should NOT send congrats again
        mock_update.callback_query.data = f"routine_check_{rid}"
        await handle_callback(mock_update, mock_context)  # toggle off
        mock_update.callback_query.data = f"routine_check_{rid}"
        await handle_callback(mock_update, mock_context)  # toggle on again

        send_calls = mock_context.bot.send_message.call_args_list
        congrats_calls = [c for c in send_calls if "routine complete" in str(c).lower()]
        assert len(congrats_calls) == 1, "Congrats should only be sent once per day"


# ── Morning prompt integration ────────────────────────────────────


class TestMorningPromptRoutine:
    @pytest.mark.asyncio
    async def test_morning_prompt_sends_routine_checklist(self, mock_context):
        """Morning prompt should send routine checklist as second message if items exist."""
        from bot.callbacks import send_morning_prompt

        db.add_routine_item("Drink water", "07:00")
        db.add_routine_item("Exercise")
        mock_context.application.bot_data = {}

        with patch("bot.callbacks.nlp.generate_fun_fact", new_callable=AsyncMock, return_value="Fun fact here"):
            await send_morning_prompt(mock_context)

        calls = mock_context.bot.send_message.call_args_list
        assert len(calls) >= 2, "Should send at least 2 messages (main + routine)"

        # Second message should be the routine checklist
        routine_msg = calls[1]
        text = routine_msg.kwargs.get("text", "") or (routine_msg.args[0] if routine_msg.args else "")
        assert "Morning Routine" in text

    @pytest.mark.asyncio
    async def test_morning_prompt_no_routine_if_empty(self, mock_context):
        """Morning prompt should NOT send routine message if no items."""
        from bot.callbacks import send_morning_prompt

        mock_context.application.bot_data = {}

        with patch("bot.callbacks.nlp.generate_fun_fact", new_callable=AsyncMock, return_value="Fun fact"):
            await send_morning_prompt(mock_context)

        calls = mock_context.bot.send_message.call_args_list
        assert len(calls) == 1, "Only main message, no routine"

    @pytest.mark.asyncio
    async def test_morning_prompt_includes_week_preview(self, mock_context):
        """Morning prompt should include week preview when tasks exist on future days."""
        from bot.callbacks import send_morning_prompt

        tomorrow = (datetime.now() + __import__("datetime").timedelta(days=1)).strftime("%Y-%m-%d")
        db.add_task("Future task", tomorrow, None)
        mock_context.application.bot_data = {}

        with patch("bot.callbacks.nlp.generate_fun_fact", new_callable=AsyncMock, return_value="Fun fact"):
            await send_morning_prompt(mock_context)

        main_msg = mock_context.bot.send_message.call_args_list[0]
        text = main_msg.kwargs.get("text", "")
        assert "This Week" in text


# ── Handler /routine command ──────────────────────────────────────


class TestRoutineHandler:
    @pytest.mark.asyncio
    async def test_routine_list_empty(self, mock_update, mock_context):
        from bot.handlers import routine_command

        mock_update.message.text = "/routine"
        await routine_command(mock_update, mock_context)

        reply = mock_update.message.reply_text
        reply.assert_called_once()
        text = reply.call_args.args[0]
        assert "No routine items" in text

    @pytest.mark.asyncio
    async def test_routine_add(self, mock_update, mock_context):
        from bot.handlers import routine_command

        mock_update.message.text = "/routine add Drink water at 7am"
        await routine_command(mock_update, mock_context)

        items = db.get_all_routine_items()
        assert len(items) == 1
        assert items[0]["description"] == "Drink water"
        assert items[0]["target_time"] == "07:00"

    @pytest.mark.asyncio
    async def test_routine_add_no_time(self, mock_update, mock_context):
        from bot.handlers import routine_command

        mock_update.message.text = "/routine add Devotion"
        await routine_command(mock_update, mock_context)

        items = db.get_all_routine_items()
        assert len(items) == 1
        assert items[0]["description"] == "Devotion"
        assert items[0]["target_time"] is None

    @pytest.mark.asyncio
    async def test_routine_add_empty_desc(self, mock_update, mock_context):
        from bot.handlers import routine_command

        mock_update.message.text = "/routine add"
        await routine_command(mock_update, mock_context)

        items = db.get_all_routine_items()
        assert len(items) == 0

    @pytest.mark.asyncio
    async def test_routine_remove_by_number(self, mock_update, mock_context):
        from bot.handlers import routine_command

        db.add_routine_item("Water")
        db.add_routine_item("Exercise")

        mock_update.message.text = "/routine remove 1"
        await routine_command(mock_update, mock_context)

        items = db.get_all_routine_items()
        assert len(items) == 1
        assert items[0]["description"] == "Exercise"

    @pytest.mark.asyncio
    async def test_routine_remove_by_name(self, mock_update, mock_context):
        from bot.handlers import routine_command

        db.add_routine_item("Morning exercise")

        mock_update.message.text = "/routine remove exercise"
        await routine_command(mock_update, mock_context)

        items = db.get_all_routine_items()
        assert len(items) == 0

    @pytest.mark.asyncio
    async def test_routine_remove_not_found(self, mock_update, mock_context):
        from bot.handlers import routine_command

        mock_update.message.text = "/routine remove nonexistent"
        await routine_command(mock_update, mock_context)

        text = mock_update.message.reply_text.call_args.args[0]
        assert "No routine item" in text

    @pytest.mark.asyncio
    async def test_routine_list_subcommand(self, mock_update, mock_context):
        from bot.handlers import routine_command

        db.add_routine_item("Water", "07:00")

        mock_update.message.text = "/routine list"
        await routine_command(mock_update, mock_context)

        text = mock_update.message.reply_text.call_args.args[0]
        assert "Water" in text
        assert "07:00" in text

    @pytest.mark.asyncio
    async def test_routine_invalid_subcommand(self, mock_update, mock_context):
        from bot.handlers import routine_command

        mock_update.message.text = "/routine foobar"
        await routine_command(mock_update, mock_context)

        text = mock_update.message.reply_text.call_args.args[0]
        assert "Usage" in text


# ── NLP routing ───────────────────────────────────────────────────


class TestRoutineNLPRouting:
    @pytest.mark.asyncio
    async def test_route_routine_add(self, mock_update, mock_context):
        from bot.handlers import _route_intent

        data = {"intent": "routine", "action": "add", "description": "Meditate", "target_time": "06:00"}
        await _route_intent(mock_update, mock_context, data, "routine")

        items = db.get_all_routine_items()
        assert len(items) == 1
        assert items[0]["description"] == "Meditate"
        assert items[0]["target_time"] == "06:00"

    @pytest.mark.asyncio
    async def test_route_routine_remove(self, mock_update, mock_context):
        from bot.handlers import _route_intent

        db.add_routine_item("Meditate")

        data = {"intent": "routine", "action": "remove", "description": "meditate"}
        await _route_intent(mock_update, mock_context, data, "routine")

        items = db.get_all_routine_items()
        assert len(items) == 0

    @pytest.mark.asyncio
    async def test_route_routine_list(self, mock_update, mock_context):
        from bot.handlers import _route_intent

        db.add_routine_item("Water")

        data = {"intent": "routine", "action": "list"}
        await _route_intent(mock_update, mock_context, data, "routine")

        text = mock_update.message.reply_text.call_args.args[0]
        assert "Water" in text

    @pytest.mark.asyncio
    async def test_route_routine_add_empty_desc(self, mock_update, mock_context):
        from bot.handlers import _route_intent

        data = {"intent": "routine", "action": "add", "description": ""}
        await _route_intent(mock_update, mock_context, data, "routine")

        items = db.get_all_routine_items()
        assert len(items) == 0
        # Should reply with error
        text = mock_update.message.reply_text.call_args.args[0]
        assert "routine item" in text.lower() or "what should" in text.lower()

    @pytest.mark.asyncio
    async def test_route_routine_remove_not_found(self, mock_update, mock_context):
        from bot.handlers import _route_intent

        data = {"intent": "routine", "action": "remove", "description": "nonexistent"}
        await _route_intent(mock_update, mock_context, data, "routine")

        text = mock_update.message.reply_text.call_args.args[0]
        assert "No routine item" in text
