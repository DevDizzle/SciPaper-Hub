"""Unit tests for the arXiv client utilities."""

from __future__ import annotations

import pytest

from service.arxiv_client import parse_arxiv_url


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://arxiv.org/abs/1234.5678", "1234.5678"),
        ("https://arxiv.org/pdf/1234.5678.pdf", "1234.5678"),
        ("https://arxiv.org/abs/1234.5678v2", "1234.5678v2"),
        ("https://arxiv.org/pdf/1234.5678v3.pdf", "1234.5678v3"),
        (" HTTP://arXiv.org/abs/1234.5678v4 ", "1234.5678v4"),
    ],
)
def test_parse_arxiv_url_valid(url: str, expected: str) -> None:
    assert parse_arxiv_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/abs/1234.5678",
        "https://arxiv.org/format/1234.5678",
        "",  # empty string should return None
    ],
)
def test_parse_arxiv_url_invalid(url: str) -> None:
    assert parse_arxiv_url(url) is None

