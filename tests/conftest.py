"""Shared fixtures for the test suite."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock

import pytest


@pytest.fixture(autouse=True)
def _patch_config(monkeypatch, tmp_path):
    """Redirect DB_PATH to a temp file so tests never touch the real database."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("bot.config.DB_PATH", db_path)
    monkeypatch.setattr("bot.database.DB_PATH", db_path)

    from bot.database import init_db
    init_db()
    yield


@pytest.fixture
def bot_data():
    """Simulates context.application.bot_data dict."""
    return {}


@pytest.fixture
def mock_context(bot_data):
    """Minimal mock of telegram ContextTypes.DEFAULT_TYPE."""
    ctx = MagicMock()
    ctx.application.bot_data = bot_data
    ctx.user_data = {}
    ctx.bot.send_message = AsyncMock()
    return ctx


@pytest.fixture
def mock_update():
    """Minimal mock of telegram Update."""
    update = MagicMock()
    update.effective_user.id = int(os.environ.get("AUTHORIZED_USER_ID", "0"))
    update.message.reply_text = AsyncMock()
    update.message.text = ""
    update.callback_query.answer = AsyncMock()
    update.callback_query.edit_message_text = AsyncMock()
    update.callback_query.edit_message_reply_markup = AsyncMock()
    update.callback_query.data = ""
    return update
