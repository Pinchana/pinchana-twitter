from pathlib import Path
from types import SimpleNamespace

import pytest
from curl_cffi.requests.errors import RequestsError
from fastapi import HTTPException
from pinchana_core.models import ScrapeRequest

from pinchana_twitter import main
from pinchana_twitter.scraper import (
    RateLimitError,
    TransientNetworkError,
    TwitterGraphQLScraper,
)


def _payload(user: dict) -> dict:
    return {
        "data": {
            "tweetResult": {
                "result": {
                    "__typename": "Tweet",
                    "rest_id": "2077331427549421918",
                    "core": {"user_results": {"result": user}},
                    "legacy": {
                        "id_str": "2077331427549421918",
                        "full_text": "Post text https://t.co/media",
                        "created_at": "Wed Jul 15 11:56:00 +0000 2026",
                        "entities": {"urls": []},
                        "extended_entities": {
                            "media": [{
                                "type": "video",
                                "url": "https://t.co/media",
                                "media_url_https": "https://pbs.twimg.com/preview.jpg",
                                "original_info": {"width": 1920, "height": 1080},
                                "video_info": {"variants": [{
                                    "content_type": "video/mp4",
                                    "bitrate": 2_000_000,
                                    "url": "https://video.twimg.com/video.mp4",
                                }]},
                            }]
                        },
                    },
                    "views": {"count": "123"},
                }
            }
        }
    }


def test_current_graphql_user_core_identity_and_video_preview():
    result = TwitterGraphQLScraper()._parse_graphql_tweet(
        _payload({
            "core": {"name": "Rick de Jager", "screen_name": "rdjgr"},
            "avatar": {"image_url": "https://pbs.twimg.com/profile_images/avatar.jpg"},
            "legacy": {"description": "Security Researcher"},
        }),
        "2077331427549421918",
    )

    assert result["author_name"] == "Rick de Jager"
    assert result["username"] == "rdjgr"
    assert result["avatar_url"] == "https://pbs.twimg.com/profile_images/avatar.jpg"
    assert result["url"].startswith("https://x.com/rdjgr/status/")
    assert result["media"][0]["thumbnail"] == "https://pbs.twimg.com/preview.jpg"
    assert result["media"][0]["looping"] is False


def test_retry_exhaustion_reraises_typed_error():
    assert TwitterGraphQLScraper.scrape_tweet.retry.reraise is True


def test_animated_gif_marker_survives_graphql_parsing():
    payload = _payload({"legacy": {"name": "Loop Author", "screen_name": "loop"}})
    payload["data"]["tweetResult"]["result"]["legacy"]["extended_entities"]["media"][0]["type"] = "animated_gif"

    result = TwitterGraphQLScraper()._parse_graphql_tweet(payload, "2077331427549421918")

    assert result["media"][0]["type"] == "video"
    assert result["media"][0]["looping"] is True


def test_note_tweet_text_replaces_truncated_legacy_preview():
    payload = _payload({"legacy": {"name": "Long Author", "screen_name": "long"}})
    result = payload["data"]["tweetResult"]["result"]
    result["legacy"]["full_text"] = "The beginning of a long post https://t.co/media"
    result["note_tweet"] = {
        "is_expandable": True,
        "note_tweet_results": {
            "result": {
                "text": "The beginning of a long post and its complete ending.",
                "entity_set": {"urls": []},
            }
        },
    }

    parsed = TwitterGraphQLScraper()._parse_graphql_tweet(
        payload,
        "2077331427549421918",
    )

    assert parsed["text"] == "The beginning of a long post and its complete ending."


def test_legacy_text_remains_fallback_for_regular_post():
    payload = _payload({"legacy": {"name": "Short Author", "screen_name": "short"}})
    payload["data"]["tweetResult"]["result"]["legacy"]["full_text"] = (
        "It's over guys https://t.co/media"
    )

    parsed = TwitterGraphQLScraper()._parse_graphql_tweet(
        payload,
        "2077331427549421918",
    )

    assert parsed["text"] == "It's over guys"


def test_legacy_graphql_identity_remains_supported():
    result = TwitterGraphQLScraper()._parse_graphql_tweet(
        _payload({"legacy": {"name": "Legacy Name", "screen_name": "legacy"}}),
        "2077331427549421918",
    )

    assert result["author_name"] == "Legacy Name"
    assert result["username"] == "legacy"


def test_quote_tweet_is_parsed_once_without_following_a_quote_chain():
    payload = _payload({"core": {"name": "Main", "screen_name": "main"}})
    result = payload["data"]["tweetResult"]["result"]
    quoted = _payload({"core": {"name": "Quoted", "screen_name": "quoted"}})["data"]["tweetResult"]["result"]
    quoted["rest_id"] = "2092341126845931776"
    quoted["legacy"]["id_str"] = "2092341126845931776"
    quoted["legacy"]["full_text"] = "quoted text"
    quoted["legacy"]["extended_entities"] = {"media": []}
    quoted["quoted_status_result"] = {"result": result}
    result["quoted_status_result"] = {"result": quoted}

    parsed = TwitterGraphQLScraper()._parse_graphql_tweet(
        payload,
        "2077331427549421918",
    )

    assert parsed["quote"]["tweet_id"] == "2092341126845931776"
    assert parsed["quote"]["text"] == "quoted text"
    assert "quote" not in parsed["quote"]


@pytest.mark.asyncio
async def test_missing_legacy_graphql_payload_falls_back(monkeypatch):
    scraper = TwitterGraphQLScraper()
    tweet_id = "2092342786641310204"

    async def fake_graphql_request(_: str) -> dict:
        return {
            "data": {
                "tweetResult": {
                    "result": {
                        "__typename": "Tweet",
                        "rest_id": tweet_id,
                    }
                }
            }
        }

    async def fake_fxtwitter_request(_: str) -> dict:
        return {
            "tweet": {
                "id": tweet_id,
                "text": "fallback text",
                "author": {
                    "name": "Skyex Summers",
                    "screen_name": "SkyexSummers",
                },
                "media": {"all": []},
            }
        }

    monkeypatch.setattr(scraper, "_graphql_tweet_request", fake_graphql_request)
    monkeypatch.setattr(scraper, "_fxtwitter_request", fake_fxtwitter_request)

    result = await scraper.scrape_tweet(tweet_id)

    assert result["source"] == "fxtwitter"
    assert result["tweet_id"] == tweet_id
    assert result["username"] == "SkyexSummers"


def test_cache_rejects_unknown_identity_and_video_without_preview(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "storage", SimpleNamespace(base_path=tmp_path))
    post = tmp_path / "1"
    post.mkdir()
    (post / "media_0.mp4").write_bytes(b"video")

    base = {
        "_cache_version": main.TWITTER_CACHE_VERSION,
        "username": "unknown",
        "video_url": "/media/twitter/1/media_0.mp4",
        "thumbnail_url": "",
        "like_count": 1,
        "looping": False,
    }
    stale_base = dict(base)
    stale_base.pop("_cache_version")
    assert not main._cached_media_ready(stale_base)
    assert not main._cached_media_ready(base)

    base["username"] = "rdjgr"
    assert not main._cached_media_ready(base)

    (post / "media_0.jpg").write_bytes(b"preview")
    base["thumbnail_url"] = "/media/twitter/1/media_0.jpg"
    assert main._cached_media_ready(base)


@pytest.mark.asyncio
async def test_video_download_keeps_video_and_preview(tmp_path, monkeypatch):
    class FakeStorage:
        base_path = tmp_path

        def prepare_post_dir(self, tweet_id: str):
            (self.base_path / tweet_id).mkdir(parents=True, exist_ok=True)

        async def download(self, url: str, destination: Path):
            destination.write_bytes(url.encode())
            return True

    monkeypatch.setattr(main, "storage", FakeStorage())

    items = await main._download_media("1", [{
        "type": "video",
        "url": "https://video.twimg.com/video.mp4",
        "thumbnail": "https://pbs.twimg.com/preview.jpg",
        "looping": True,
    }])

    assert len(items) == 1
    assert items[0].video_url == "/media/twitter/1/media_0.mp4"
    assert items[0].thumbnail_url == "/media/twitter/1/media_0.jpg"
    assert items[0].looping is True
    assert (tmp_path / "1" / "media_0.mp4").is_file()
    assert (tmp_path / "1" / "media_0.jpg").is_file()

    quoted = await main._download_media(
        "1",
        [{"type": "image", "url": "https://pbs.twimg.com/quoted.jpg"}],
        filename_prefix="quote_",
    )

    assert quoted[0].thumbnail_url == "/media/twitter/1/quote_media_0.jpg"
    assert (tmp_path / "1" / "media_0.mp4").is_file()
    assert (tmp_path / "1" / "media_0.jpg").is_file()
    assert (tmp_path / "1" / "quote_media_0.jpg").is_file()


@pytest.mark.asyncio
async def test_incomplete_media_download_is_not_accepted(monkeypatch):
    async def incomplete_download(*_args, **_kwargs):
        return []

    monkeypatch.setattr(main, "_download_media", incomplete_download)

    with pytest.raises(main.MediaDownloadError):
        await main._response_from_parsed(
            "1",
            {
                "tweet_id": "1",
                "username": "pinchana",
                "media": [{"type": "image", "url": "https://cdn.example/image.jpg"}],
            },
            download_media=True,
        )


@pytest.mark.asyncio
async def test_guest_activation_dns_timeout_is_retryable():
    class FailingSession:
        async def post(self, *_args, **_kwargs):
            raise RequestsError(
                "Resolving timed out after 15000 milliseconds",
                code=28,
            )

    with pytest.raises(TransientNetworkError, match="Resolving timed out"):
        await TwitterGraphQLScraper()._activate_guest_token(FailingSession())


@pytest.mark.asyncio
async def test_guest_activation_rate_limit_is_not_swallowed():
    class RateLimitedResponse:
        status_code = 429

    class RateLimitedSession:
        async def post(self, *_args, **_kwargs):
            return RateLimitedResponse()

    with pytest.raises(RateLimitError):
        await TwitterGraphQLScraper()._activate_guest_token(RateLimitedSession())


@pytest.mark.asyncio
async def test_transient_network_failure_maps_to_retryable_503(monkeypatch):
    monkeypatch.setattr(main.storage, "is_cached", lambda _tweet_id: False)

    async def failed_scrape(_tweet_id: str):
        raise TransientNetworkError("DNS resolution failed")

    monkeypatch.setattr(main, "_scrape_tweet", failed_scrape)

    with pytest.raises(HTTPException) as exc_info:
        await main._process_scrape_request(
            ScrapeRequest(url="https://x.com/pinchana/status/2095262892224328187")
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["code"] == "upstream_unavailable"
