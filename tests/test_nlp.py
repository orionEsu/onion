"""Tests for NLP helper functions (no LLM calls required)."""

import json
import pytest

from bot.nlp import _strip_fences, _extract_json
from bot.models import ParsedTask


class TestStripFences:
    def test_no_fences(self):
        assert _strip_fences('{"key": "val"}') == '{"key": "val"}'

    def test_json_fences(self):
        assert _strip_fences('```json\n{"key": "val"}\n```') == '{"key": "val"}'

    def test_plain_fences(self):
        assert _strip_fences('```\n{"key": "val"}\n```') == '{"key": "val"}'

    def test_whitespace(self):
        result = _strip_fences('  {"key": "val"}  ')
        assert result == '{"key": "val"}'


class TestExtractJson:
    def test_clean_json(self):
        result = _extract_json('{"intent": "add_task"}')
        assert result["intent"] == "add_task"

    def test_json_with_fences(self):
        result = _extract_json('```json\n{"intent": "help"}\n```')
        assert result["intent"] == "help"

    def test_json_with_surrounding_text(self):
        result = _extract_json('Here is the result: {"intent": "done", "task_id": 5} end')
        assert result["intent"] == "done"
        assert result["task_id"] == 5

    def test_json_array(self):
        result = _extract_json('[{"description": "task1"}, {"description": "task2"}]')
        assert isinstance(result, list)
        assert len(result) == 2

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _extract_json("no json here at all")

    def test_nested_braces_in_strings(self):
        raw = '{"intent": "add_task", "description": "fix {bug} in code"}'
        result = _extract_json(raw)
        assert result["description"] == "fix {bug} in code"

    def test_compound_intent(self):
        raw = '{"intent": "compound", "actions": [{"intent": "done", "task_id": 1}, {"intent": "add_task", "description": "New"}]}'
        result = _extract_json(raw)
        assert result["intent"] == "compound"
        assert len(result["actions"]) == 2


class TestParsedTaskModel:
    def test_defaults(self):
        task = ParsedTask(description="Test", due_date="2026-03-10")
        assert task.due_time is None
        assert task.confidence == 1.0
        assert task.recurrence_rule is None
        assert task.label_names == []
        assert task.notes is None

    def test_all_fields(self):
        task = ParsedTask(
            description="Gym",
            due_date="2026-03-10",
            due_time="07:00",
            confidence=0.9,
            recurrence_rule="weekly:monday",
            label_names=["Health"],
            notes="Leg day",
        )
        assert task.description == "Gym"
        assert task.due_time == "07:00"
        assert task.recurrence_rule == "weekly:monday"
        assert task.notes == "Leg day"
