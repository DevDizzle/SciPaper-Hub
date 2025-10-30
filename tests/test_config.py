"""Tests for configuration helpers."""

from __future__ import annotations

import pytest

from common import config


def test_get_settings_missing_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in config.REQUIRED_VARS:
        monkeypatch.delenv(var, raising=False)

    config.get_settings.cache_clear()

    with pytest.raises(RuntimeError):
        config.get_settings()

    config.get_settings.cache_clear()

