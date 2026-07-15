"""Twitter/X scraper plugin — mounts as a FastAPI router."""

from __future__ import annotations

import asyncio
import logging
import os
import re
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
    like_count: Optional[int] = None
    reply_count: Optional[int] = None
    repost_count: Optional[int] = None
    quote_count: Optional[int] = None
    view_count: Optional[int] = None
    link: Optional[str] = None
    nsfw: bool = False
    source: Optional[str] = None
    created_at: Optional[str] = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
scraper = TwitterGraphQLScraper()
gluetun = GluetunController()
storage = MediaStorage(
    base_path=os.getenv("CACHE_PATH", "./cache"),
    max_size_gb=float(os.getenv("CACHE_MAX_SIZE_GB", "10.0")),
)


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

    # Invalidate old cache entries that predate engagement stat fields
    engagement_keys = ("like_count", "reply_count", "repost_count", "view_count", "username")
    if not any(k in metadata for k in engagement_keys):
        return False

    urls: list[str] = []
    for key in ("thumbnail_url", "video_url"):
        value = metadata.get(key)
        if value:
            urls.append(value)

    carousel = metadata.get("carousel") or []
    if isinstance(carousel, list):
        for item in carousel:
            if not isinstance(item, dict):
                continue
            for key in ("thumbnail_url", "video_url"):
                value = item.get(key)
                if value:
                    urls.append(value)

    for url in urls:
        path = _media_url_to_path(url)
        if not path or not path.exists():
            return False

    return True


async def _download_media(tweet_id: str, media_list: list[dict]) -> list[MediaItem]:
    storage.prepare_post_dir(tweet_id)

    tasks = []
    mapping: list[tuple[int, str]] = []

    for idx, item in enumerate(media_list):
        media_url = item.get("url")
        if not media_url:
            continue
        ext = "mp4" if item.get("type") == "video" else "jpg"
        dest = storage.base_path / tweet_id / f"media_{idx}.{ext}"
        tasks.append(storage.download(media_url, dest))
        mapping.append((idx, ext))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.error("Twitter media download error: %s", r)

    items: list[MediaItem] = []
    for idx, ext in mapping:
        items.append(
            MediaItem(
                index=idx,
                media_type="video" if ext == "mp4" else "image",
                thumbnail_url=f"/media/twitter/{tweet_id}/media_{idx}.jpg" if ext == "jpg" else "",
                video_url=f"/media/twitter/{tweet_id}/media_{idx}.mp4" if ext == "mp4" else None,
            )
        )
    return items


async def _scrape_tweet(tweet_id: str) -> TwitterScrapeResponse:
    parsed = await scraper.scrape_tweet(tweet_id)
    media_items = await _download_media(tweet_id, parsed.get("media") or [])

    if media_items:
        media_type = "video" if any(m.media_type == "video" for m in media_items) else "image"
        thumbnail_url = media_items[0].thumbnail_url or ""
        video_url = media_items[0].video_url
        carousel = media_items if len(media_items) > 1 else None
    else:
        media_type = "text"
        thumbnail_url = ""
        video_url = None
        carousel = None

    response = TwitterScrapeResponse(
        shortcode=tweet_id,
        caption=parsed.get("text") or "",
        author=parsed.get("username") or parsed.get("author_name") or "unknown",
        media_type=media_type,
        thumbnail_url=thumbnail_url,
        video_url=video_url,
        carousel=carousel,
        username=parsed.get("username"),
        author_name=parsed.get("author_name"),
        like_count=parsed.get("like_count"),
        reply_count=parsed.get("reply_count"),
        repost_count=parsed.get("repost_count"),
        quote_count=parsed.get("quote_count"),
        view_count=parsed.get("view_count"),
        link=parsed.get("link"),
        nsfw=bool(parsed.get("nsfw")),
        source=parsed.get("source"),
        created_at=(str(parsed["created_at"]) if parsed.get("created_at") is not None else None),
    )

    storage.save_metadata(tweet_id, response.model_dump())
    return response


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
