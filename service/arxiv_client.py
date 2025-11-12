"""Client for the arXiv Atom API."""

from __future__ import annotations

import re
import time
from typing import Dict, Iterable, List, Optional
from xml.etree import ElementTree as ET

import requests

ARXIV_API_URL = "http://export.arxiv.org/api/query"
_RATE_LIMIT_SECONDS = 3.0
_last_request_ts: float = 0.0


class ArxivNotFoundError(RuntimeError):
    """Raised when the arXiv API does not return a result for a query."""


def _rate_limit_sleep() -> None:
    global _last_request_ts
    elapsed = time.time() - _last_request_ts
    if elapsed < _RATE_LIMIT_SECONDS:
        time.sleep(_RATE_LIMIT_SECONDS - elapsed)
    _last_request_ts = time.time()


def _parse_summary(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def _extract_authors(entry: ET.Element) -> List[str]:
    authors = []
    for author in entry.findall("{http://www.w3.org/2005/Atom}author"):
        name = author.findtext("{http://www.w3.org/2005/Atom}name")
        if name:
            authors.append(name)
    return authors


def _extract_categories(entry: ET.Element) -> List[str]:
    categories = []
    for cat in entry.findall("{http://www.w3.org/2005/Atom}category"):
        term = cat.attrib.get("term")
        if term:
            categories.append(term)
    return categories


def _extract_links(entry: ET.Element) -> Dict[str, str]:
    links = {}
    for link in entry.findall("{http://www.w3.org/2005/Atom}link"):
        rel = link.attrib.get("rel")
        href = link.attrib.get("href")
        if not rel or not href:
            continue
        if rel.endswith("/abs") or rel == "alternate":
            links["abs"] = href
        elif rel.endswith("/pdf") or link.attrib.get("title") == "pdf":
            links["pdf"] = href
    return links


def _parse_version(arxiv_id: str) -> int:
    match = re.search(r"v(\d+)$", arxiv_id)
    return int(match.group(1)) if match else 1


def parse_entry(entry: ET.Element) -> Dict[str, object]:
    """Convert an Atom ``<entry>`` element to a normalized dictionary."""

    ns = "{http://www.w3.org/2005/Atom}"
    arxiv_id = entry.findtext(f"{ns}id")
    if not arxiv_id:
        raise ValueError("Entry missing id field")
    arxiv_id = arxiv_id.rsplit("/", maxsplit=1)[-1]
    base_id = arxiv_id.split("v")[0]

    title = entry.findtext(f"{ns}title") or ""
    summary = entry.findtext(f"{ns}summary") or ""
    published_at = entry.findtext(f"{ns}published") or ""
    updated_at = entry.findtext(f"{ns}updated") or ""

    categories = _extract_categories(entry)
    primary_category = categories[0] if categories else ""

    return {
        "arxiv_id": arxiv_id,
        "base_id": base_id,
        "version": _parse_version(arxiv_id),
        "title": re.sub(r"\s+", " ", title.strip()),
        "abstract": _parse_summary(summary),
        "authors": _extract_authors(entry),
        "categories": categories,
        "primary_category": primary_category,
        "published_at": published_at,
        "updated_at": updated_at,
        "links": _extract_links(entry),
    }


def get_by_id(arxiv_id: str) -> Dict[str, object]:
    """Fetch a single arXiv entry by id."""

    global _last_request_ts
    params: Dict[str, str | int] = {
        "search_query": f"id:{arxiv_id}",
        "start": 0,
        "max_results": 1,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    _rate_limit_sleep()
    response = requests.get(ARXIV_API_URL, params=params, timeout=60)
    response.raise_for_status()

    feed = ET.fromstring(response.content)
    entries = feed.findall("{http://www.w3.org/2005/Atom}entry")
    if not entries:
        raise ArxivNotFoundError(f"No entry found for id {arxiv_id}")
    return parse_entry(entries[0])


def iter_query(params: Dict[str, str | int]) -> Iterable[Dict[str, object]]:
    """Iterate over entries for an arbitrary arXiv query."""

    _rate_limit_sleep()
    response = requests.get(ARXIV_API_URL, params=params, timeout=60)
    response.raise_for_status()
    feed = ET.fromstring(response.content)
    for entry in feed.findall("{http://www.w3.org/2005/Atom}entry"):
        yield parse_entry(entry)


ARXIV_URL_PATTERN = re.compile(
    r"https?://arxiv\.org/(?:abs|pdf)/(?P<identifier>[\w.\-]+)(?:v\d+)?(?:\.pdf)?",
    re.IGNORECASE,
)


def parse_arxiv_url(url: str) -> Optional[str]:
    """Extract the arXiv identifier from ``/abs`` or ``/pdf`` URLs."""

    match = ARXIV_URL_PATTERN.match(url.strip())
    if not match:
        return None
    identifier = match.group("identifier")
    if identifier.lower().endswith(".pdf"):
        identifier = identifier[: -len(".pdf")]
    version_match = re.search(r"v\d+", url)
    if version_match and version_match.group() not in identifier:
        identifier = f"{identifier}{version_match.group()}"
    return identifier


__all__ = [
    "ARXIV_API_URL",
    "ArxivNotFoundError",
    "parse_entry",
    "get_by_id",
    "iter_query",
    "parse_arxiv_url",
]
