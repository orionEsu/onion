"""Tests for shared utility functions."""

import time
import pytest
from unittest.mock import MagicMock

from bot.utils import store_undo, task_to_dict


class TestStoreUndo:
    def test_stores_undo_data(self, mock_context):
        store_undo(mock_context, "done", 5, {"description": "Task"})
        undo = mock_context.application.bot_data["last_undo"]
        assert undo["type"] == "done"
        assert undo["task_id"] == 5
        assert undo["previous_state"]["description"] == "Task"
        assert time.time() - undo["timestamp"] < 2

    def test_overwrites_previous_undo(self, mock_context):
        store_undo(mock_context, "done", 1, {"description": "First"})
        store_undo(mock_context, "delete", 2, {"description": "Second"})
        undo = mock_context.application.bot_data["last_undo"]
        assert undo["type"] == "delete"
        assert undo["task_id"] == 2


class TestTaskToDict:
    def test_converts_row_to_dict(self):
        # Simulate a sqlite3.Row-like object
        class FakeRow:
            def __init__(self, data):
                self._data = data
            def keys(self):
                return self._data.keys()
            def __getitem__(self, key):
                return self._data[key]

        row = FakeRow({"id": 1, "description": "Task", "status": "pending"})
        result = task_to_dict(row)
        assert isinstance(result, dict)
        assert result["id"] == 1
        assert result["description"] == "Task"
        assert result["status"] == "pending"
