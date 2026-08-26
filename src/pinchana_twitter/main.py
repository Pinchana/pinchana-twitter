"""Twitter/X scraper plugin — mounts as a FastAPI router."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import OrderedDict
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import FileResponse
from pinchana_core.models import MediaItem, ScrapeRequest, ScrapeResponse
from pinchana_core.plugins import ScraperPlugin, registry
from pinchana_core.storage import MediaStorage
from pinchana_core.vpn import GluetunController, VpnRotationError

from .scraper import NotFoundError, RateLimitError, ScraperError, TwitterGraphQLScraper


class TwitterScrapeResponse(ScrapeResponse):
    """Extended response for Twitter/X including engagement + metadata."""

    username: Optional[str] = None
    author_name: Optional[str] = None
    avatar_url: Optional[str] = None
    like_count: Optional[int] = None
    reply_count: Optional[int] = None
    repost_count: Optional[int] = None
    quote_count: Optional[int] = None
    view_count: Optional[int] = None
    link: Optional[str] = None
    nsfw: bool = False
    source: Optional[str] = None
    created_at: Optional[str] = None
    looping: bool = False
    quote: Optional[dict] = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
scraper = TwitterGraphQLScraper()
gluetun = GluetunController()
storage = MediaStorage(
    base_path=os.getenv("CACHE_PATH", "./cache"),
    max_size_gb=float(os.getenv("CACHE_MAX_SIZE_GB", "10.0")),
)

# Increment when cached response semantics change. Metadata written before
# Note Tweet support may contain a permanently truncated caption.
TWITTER_CACHE_VERSION = 4


class _InspectionCache:
    def __init__(self, ttl: float = 300, max_entries: int = 256):
        self.ttl = ttl
        self.max_entries = max_entries
        self.entries: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> dict | None:
        async with self.lock:
            entry = self.entries.pop(key, None)
            if not entry or time.monotonic() - entry[0] > self.ttl:
                return None
            self.entries[key] = entry
            return entry[1]

    async def put(self, key: str, value: dict) -> None:
        async with self.lock:
            self.entries.pop(key, None)
            self.entries[key] = (time.monotonic(), value)
            while len(self.entries) > self.max_entries:
                self.entries.popitem(last=False)


inspection_cache = _InspectionCache()


TWITTER_URL_RE = re.compile(
    r"https?://(?:www\.|mobile\.|vxtwitter\.|fxtwitter\.)?(?:twitter|x)\.com/[\w.-]+/status/(\d+)",
    re.IGNORECASE,
)


def extract_tweet_id(url: str) -> str:
    match = TWITTER_URL_RE.search(str(url))
    if not match:
        raise HTTPException(status_code=400, detail="Invalid Twitter/X URL format")
    return match.group(1)


def _media_url_to_path(url: str | None):
    if not url:
        return None
    url = str(url)
    if not url.startswith("/media/"):
        return None
    path_part = url.split("?", 1)[0][len("/media/") :]
    parts = path_part.split("/", 2)
    if len(parts) < 3:
        return None
    platform, post_id, filename = parts[0], parts[1], parts[2]
    if platform != "twitter" or not post_id or not filename:
        return None
    return storage.base_path / post_id / filename


def _cached_media_ready(metadata: dict) -> bool:
    if not isinstance(metadata, dict):
        return False
    if metadata.get("_cache_version") != TWITTER_CACHE_VERSION:
        return False

    # Invalidate old cache entries that predate engagement stat fields
    engagement_keys = ("like_count", "reply_count", "repost_count", "view_count", "username")
    if not any(k in metadata for k in engagement_keys):
        return False
    if str(metadata.get("username") or "").strip().lower() in {"", "unknown"}:
        return False
    if "looping" not in metadata:
        return False

    urls: list[str] = []
    payloads = [metadata]
    if isinstance(metadata.get("quote"), dict):
        payloads.append(metadata["quote"])
    for payload in payloads:
        if not _payload_media_ready(payload, urls):
            return False

    for url in urls:
        path = _media_url_to_path(url)
        if not path or not path.exists():
            return False

    return True


def _payload_media_ready(metadata: dict, urls: list[str]) -> bool:
    top_thumbnail = metadata.get("thumbnail_url")
    top_video = metadata.get("video_url")
    if top_video and not top_thumbnail:
        return False
    for value in (top_thumbnail, top_video):
        if value:
            urls.append(value)

    carousel = metadata.get("carousel") or []
    if isinstance(carousel, list):
        for item in carousel:
            if not isinstance(item, dict):
                continue
            if item.get("video_url") and "looping" not in item:
                return False
            if item.get("video_url") and not item.get("thumbnail_url"):
                return False
            for key in ("thumbnail_url", "video_url"):
                value = item.get(key)
                if value:
                    urls.append(value)

    return True


async def _download_media(
    tweet_id: str,
    media_list: list[dict],
    *,
    filename_prefix: str = "",
) -> list[MediaItem]:
    storage.prepare_post_dir(tweet_id)

    tasks = []
    destinations: list[tuple[int, str, Path]] = []

    for idx, item in enumerate(media_list):
        media_url = item.get("url")
        if not media_url:
            continue
        ext = "mp4" if item.get("type") == "video" else "jpg"
        dest = storage.base_path / tweet_id / f"{filename_prefix}media_{idx}.{ext}"
        tasks.append(storage.download(media_url, dest))
        destinations.append((idx, ext, dest))

        preview_url = item.get("thumbnail") if ext == "mp4" else None
        if preview_url:
            preview_dest = storage.base_path / tweet_id / f"{filename_prefix}media_{idx}.jpg"
            tasks.append(storage.download(preview_url, preview_dest))
            destinations.append((idx, "jpg", preview_dest))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed_destinations: set[Path] = set()
    for destination, result in zip(destinations, results):
        if isinstance(result, Exception) or result is not True:
            failed_destinations.add(destination[2])
            logger.error(
                "Twitter media download error index=%s type=%s: %s",
                destination[0],
                destination[1],
                result,
            )

    items: list[MediaItem] = []
    for idx, item in enumerate(media_list):
        is_video = item.get("type") == "video"
        primary_ext = "mp4" if is_video else "jpg"
        primary = storage.base_path / tweet_id / f"{filename_prefix}media_{idx}.{primary_ext}"
        if (
            primary in failed_destinations
            or not primary.is_file()
            or primary.stat().st_size == 0
        ):
            continue
        preview = storage.base_path / tweet_id / f"{filename_prefix}media_{idx}.jpg"
        items.append(
            MediaItem(
                index=idx,
                media_type="video" if is_video else "image",
                thumbnail_url=(
                    f"/media/twitter/{tweet_id}/{filename_prefix}media_{idx}.jpg"
                    if (
                        preview not in failed_destinations
                        and preview.is_file()
                        and preview.stat().st_size > 0
                    )
                    else ""
                ),
                video_url=(
                    f"/media/twitter/{tweet_id}/{filename_prefix}media_{idx}.mp4"
                    if is_video
                    else None
                ),
                looping=bool(item.get("looping")),
            )
        )
    return items


async def _parsed_tweet(tweet_id: str) -> dict:
    parsed = await inspection_cache.get(tweet_id)
    if parsed is None:
        parsed = await scraper.scrape_tweet(tweet_id)
        await inspection_cache.put(tweet_id, parsed)
    return parsed


async def _response_from_parsed(
    tweet_id: str,
    parsed: dict,
    *,
    download_media: bool,
    filename_prefix: str = "",
) -> TwitterScrapeResponse:
    media_items = (
        await _download_media(
            tweet_id,
            parsed.get("media") or [],
            filename_prefix=filename_prefix,
        )
        if download_media
        else []
    )

    if media_items:
        media_type = "video" if any(m.media_type == "video" for m in media_items) else "image"
        thumbnail_url = media_items[0].thumbnail_url or ""
        video_url = media_items[0].video_url
        carousel = media_items if len(media_items) > 1 else None
        looping = bool(media_items[0].looping) if len(media_items) == 1 else False
    else:
        media_type = "text"
        thumbnail_url = ""
        video_url = None
        carousel = None
        looping = False

    quote_parsed = parsed.get("quote")
    quote = None
    if isinstance(quote_parsed, dict):
        quote = (
            await _response_from_parsed(
                tweet_id,
                quote_parsed,
                download_media=download_media,
                filename_prefix="quote_",
            )
        ).model_dump(exclude={"quote"})
        quote["source_url"] = quote_parsed.get("url")

    return TwitterScrapeResponse(
        shortcode=str(parsed.get("tweet_id") or tweet_id),
        caption=parsed.get("text") or "",
        author=parsed.get("username") or parsed.get("author_name") or "unknown",
        media_type=media_type,
        thumbnail_url=thumbnail_url,
        video_url=video_url,
        carousel=carousel,
        username=parsed.get("username"),
        author_name=parsed.get("author_name"),
        avatar_url=parsed.get("avatar_url"),
        like_count=parsed.get("like_count"),
        reply_count=parsed.get("reply_count"),
        repost_count=parsed.get("repost_count"),
        quote_count=parsed.get("quote_count"),
        view_count=parsed.get("view_count"),
        link=parsed.get("link"),
        nsfw=bool(parsed.get("nsfw")),
        source=parsed.get("source"),
        created_at=(str(parsed["created_at"]) if parsed.get("created_at") is not None else None),
        looping=looping,
        quote=quote,
    )


async def _scrape_tweet(tweet_id: str) -> TwitterScrapeResponse:
    parsed = await _parsed_tweet(tweet_id)
    response = await _response_from_parsed(tweet_id, parsed, download_media=True)

    metadata = response.model_dump()
    metadata["_cache_version"] = TWITTER_CACHE_VERSION
    storage.save_metadata(tweet_id, metadata)
    return response


async def _inspect_tweet(tweet_id: str) -> TwitterScrapeResponse:
    parsed = await _parsed_tweet(tweet_id)
    return await _response_from_parsed(tweet_id, parsed, download_media=False)


async def _process_scrape_request(request: ScrapeRequest):
    tweet_id = extract_tweet_id(str(request.url))

    if storage.is_cached(tweet_id):
        cached = storage.load_metadata(tweet_id)
        if cached and _cached_media_ready(cached):
            logger.info("Cache hit for tweet %s", tweet_id)
            return TwitterScrapeResponse(**cached)
        logger.info("Cache invalid for tweet %s, re-scraping", tweet_id)

    last_error: Exception | None = None

    try:
        # The scraper now has @retry with automatic VPN rotation
        return await _scrape_tweet(tweet_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RateLimitError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error("Scrape failed after retries: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape", response_model=TwitterScrapeResponse)
async def process_scrape_request(request: ScrapeRequest):
    tweet_id = extract_tweet_id(str(request.url))
    return await storage.singleflight(tweet_id, lambda: _process_scrape_request(request))


@router.post("/inspect", response_model=TwitterScrapeResponse)
async def inspect_tweet_request(request: ScrapeRequest):
    tweet_id = extract_tweet_id(str(request.url))
    try:
        return await storage.singleflight(
            f"inspect:{tweet_id}",
            lambda: _inspect_tweet(tweet_id),
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RateLimitError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/media/{platform}/{post_id}/{filename:path}")
async def serve_media(platform: str, post_id: str, filename: str):
    if platform != "twitter":
        raise HTTPException(status_code=404, detail="Invalid platform")
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=404, detail="Invalid path")

    file_path = storage.base_path / post_id / filename
    resolved = file_path.resolve()
    base_resolved = storage.base_path.resolve()
    if not str(resolved).startswith(str(base_resolved)):
        raise HTTPException(status_code=404, detail="Invalid path")

    if not resolved.exists() or not resolved.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(resolved)


@router.get("/health")
async def health_check():
    try:
        status = await gluetun.get_vpn_status()
        vpn_status = status.get("status", "").lower()
        if gluetun.enabled and vpn_status != "running":
            raise HTTPException(status_code=503, detail=f"VPN not running: {vpn_status}")
        return {"status": "healthy", "service": "twitter", "vpn": status}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"VPN check failed: {e}")


registry.register(
    ScraperPlugin(
        name="twitter",
        router=router,
        route_patterns=["x.com", "twitter.com"],
    )
)

app = FastAPI(title="Pinchana Twitter", version="0.1.0")
app.include_router(router)


@app.on_event("shutdown")
async def close_storage_client():
    await storage.close()
