import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from aggregator.aggregator import compute_md5, is_duplicate, is_stop_word, parse_rss_entry


# --- is_stop_word ---

def test_hard_stopword_detected():
    assert is_stop_word("Новый товар erid: 12345", "it") is True


def test_hard_stopword_case_insensitive():
    assert is_stop_word("Это РЕКЛАМА нашего партнёра", "cooking") is True


def test_topic_stopword_it():
    assert is_stop_word("Открытая вакансия senior python developer", "it") is True


def test_topic_stopword_cooking():
    assert is_stop_word("Доставка еды за 30 минут!", "cooking") is True


def test_clean_text_passes():
    assert is_stop_word("Интересная статья о новых технологиях", "it") is False


def test_unknown_topic_only_hard():
    assert is_stop_word("промокод на скидку", "unknown_topic") is True
    assert is_stop_word("Просто текст", "unknown_topic") is False


# --- parse_rss_entry ---

def _entry(**kwargs):
    return kwargs


def test_parse_rss_entry_with_enclosure():
    entry = _entry(
        summary="Текст поста",
        link="https://t.me/channel/123",
        enclosures=[{"href": "https://example.com/img.jpg"}],
    )
    text, media_url, source_url = parse_rss_entry(entry)
    assert text == "Текст поста"
    assert media_url == "https://example.com/img.jpg"
    assert source_url == "https://t.me/channel/123"


def test_parse_rss_entry_no_media():
    entry = _entry(summary="Только текст", link="https://t.me/x/1", enclosures=[])
    text, media_url, source_url = parse_rss_entry(entry)
    assert text == "Только текст"
    assert media_url is None


def test_parse_rss_entry_strips_html():
    entry = _entry(
        summary="<b>Жирный</b> текст <a href='#'>ссылка</a>",
        link="https://t.me/x/2",
        enclosures=[],
    )
    text, _, _ = parse_rss_entry(entry)
    assert "<" not in text
    assert "Жирный" in text


def test_parse_rss_entry_empty_description():
    entry = _entry(link="https://t.me/x/3", enclosures=[])
    text, media_url, _ = parse_rss_entry(entry)
    assert text == ""
    assert media_url is None


def test_parse_rss_entry_media_content():
    entry = _entry(
        summary="Текст",
        link="https://t.me/x/4",
        enclosures=[],
        media_content=[{"url": "https://example.com/photo.jpg"}],
    )
    _, media_url, _ = parse_rss_entry(entry)
    assert media_url == "https://example.com/photo.jpg"


def test_parse_rss_entry_media_thumbnail():
    entry = _entry(
        summary="Текст",
        link="https://t.me/x/5",
        enclosures=[],
        media_content=[],
        media_thumbnail=[{"url": "https://example.com/thumb.jpg"}],
    )
    _, media_url, _ = parse_rss_entry(entry)
    assert media_url == "https://example.com/thumb.jpg"


def test_parse_rss_entry_img_in_html():
    entry = _entry(
        summary='<p>Текст</p><img src="https://example.com/inline.jpg" />',
        link="https://t.me/x/6",
        enclosures=[],
    )
    text, media_url, _ = parse_rss_entry(entry)
    assert media_url == "https://example.com/inline.jpg"
    assert "<" not in text


def test_parse_rss_entry_enclosure_takes_priority():
    entry = _entry(
        summary='<img src="https://example.com/inline.jpg" />',
        link="https://t.me/x/7",
        enclosures=[{"href": "https://example.com/enclosure.jpg"}],
        media_content=[{"url": "https://example.com/media.jpg"}],
    )
    _, media_url, _ = parse_rss_entry(entry)
    assert media_url == "https://example.com/enclosure.jpg"


# --- is_duplicate (async) ---

@pytest.mark.asyncio
async def test_is_duplicate_known_md5():
    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=("row",))

    db = MagicMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=cursor)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.execute = MagicMock(return_value=ctx)

    result = await is_duplicate("known_md5", "https://example.com/new", db)
    assert result is True


@pytest.mark.asyncio
async def test_is_duplicate_new_post():
    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)

    db = MagicMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=cursor)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.execute = MagicMock(return_value=ctx)

    result = await is_duplicate("new_md5", "https://example.com/brand-new", db)
    assert result is False


# --- compute_md5 ---

def test_compute_md5_deterministic():
    assert compute_md5("hello") == compute_md5("hello")


def test_compute_md5_different_inputs():
    assert compute_md5("a") != compute_md5("b")
