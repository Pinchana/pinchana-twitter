"""Twitter/X scraper using internal GraphQL guest flow with fallback providers."""

from __future__ import annotations

import json
import logging
import urllib.parse
from typing import Any, Optional

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.errors import RequestsError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from pinchana_core.vpn import GluetunController, VpnRotationError

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    pass


class RateLimitError(ScraperError):
    pass


class TransientNetworkError(ScraperError):
    """Retryable DNS, connection, or timeout failure."""


class NotFoundError(ScraperError):
    pass


gluetun = GluetunController()


async def trigger_rotation(retry_state):
    """Trigger VPN IP rotation before each retry."""
    failure = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "Retry attempt %s after %s. Reconnecting VPN...",
        retry_state.attempt_number,
        type(failure).__name__ if failure else "upstream failure",
    )
    try:
        await gluetun.rotate_ip(
            wait_for_cooldown=True,
            reason=type(failure).__name__ if failure else "Twitter upstream failure",
        )
    except VpnRotationError as e:
        logger.warning(f"VPN rotation failed: {e}")


class TwitterGraphQLScraper:
    """Tweet extractor with GraphQL-first strategy.

    Primary path:
      - Activate guest token
      - Call TweetResultByRestId GraphQL endpoint

    Fallback path:
      - api.fxtwitter.com/status/{tweet_id}
    """

    BEARER_TOKEN = (
        "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs="
        "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
    )

    GUEST_ACTIVATE_ENDPOINTS = [
        "https://api.x.com/1.1/guest/activate.json",
        "https://api.twitter.com/1.1/guest/activate.json",
    ]

    OPENAPI_PLACEHOLDER_URL = (
        "https://raw.githubusercontent.com/fa0311/twitter-openapi/"
        "refs/heads/main/src/config/placeholder.json"
    )

    # Known-good fallback query ids for TweetResultByRestId, newest first.
    FALLBACK_QUERY_IDS = [
        "tCVRZ3WCvoj0BVO7BKnL-Q",
        "zy39CwTyYhU-_0LP7dljjg",
        "7xflPyRiUxGVbJd4uWmbfg",
    ]

    DEFAULT_VARIABLES = {
        "tweetId": "0",
        "withCommunity": False,
        "includePromotedContent": False,
        "withVoice": False,
    }

    DEFAULT_FEATURES = {
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "articles_preview_enabled": True,
        "tweetypie_unmention_optimization_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "creator_subscriptions_quote_tweet_preview_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "tweet_with_visibility_results_prefer_gql_media_interstitial_enabled": True,
        "rweb_video_timestamps_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "rweb_tipjar_consumption_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
        "payments_enabled": False,
        "premium_content_api_read_enabled": False,
        "profile_label_improvements_pcf_label_in_post_enabled": True,
        "responsive_web_grok_analysis_button_from_backend": True,
        "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
        "responsive_web_grok_analyze_post_followups_enabled": True,
        "responsive_web_grok_community_note_auto_translation_is_enabled": False,
        "responsive_web_grok_image_annotation_enabled": True,
        "responsive_web_grok_imagine_annotation_enabled": True,
        "responsive_web_grok_share_attachment_enabled": True,
        "responsive_web_grok_show_grok_translated_post": False,
        "responsive_web_jetfuel_frame": True,
        "responsive_web_profile_redirect_enabled": False,
    }

    DEFAULT_FIELD_TOGGLES = {
        "withArticleRichContentState": True,
        "withArticlePlainText": False,
    }

    def __init__(self):
        self._query_id: Optional[str] = None
        self._variables: dict[str, Any] = dict(self.DEFAULT_VARIABLES)
        self._features: dict[str, Any] = dict(self.DEFAULT_FEATURES)
        self._field_toggles: dict[str, Any] = dict(self.DEFAULT_FIELD_TOGGLES)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=4, max=30),
        retry=retry_if_exception_type((RateLimitError, TransientNetworkError)),
        before_sleep=trigger_rotation,
        reraise=True,
    )
    async def scrape_tweet(self, tweet_id: str) -> dict:
        """Scrape a public tweet by numeric rest_id."""
        # Primary: GraphQL
        try:
            raw = await self._graphql_tweet_request(tweet_id)
            return self._parse_graphql_tweet(raw, tweet_id)
        except NotFoundError:
            raise
        except Exception as e:
            logger.warning("GraphQL path failed for %s: %s", tweet_id, e)

        # Fallback: FxTwitter API
        try:
            raw = await self._fxtwitter_request(tweet_id)
            return self._parse_fxtwitter_tweet(raw, tweet_id)
        except Exception as e:
            logger.warning("FxTwitter fallback failed for %s: %s", tweet_id, e)
            raise

    # ------------------------------------------------------------------
    # GraphQL request flow
    # ------------------------------------------------------------------

    async def _activate_guest_token(self, session: AsyncSession) -> str:
        headers = {
            "Authorization": f"Bearer {self.BEARER_TOKEN}",
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        }

        last_error = None
        for endpoint in self.GUEST_ACTIVATE_ENDPOINTS:
            try:
                resp = await session.post(endpoint, headers=headers, timeout=15)
                if resp.status_code == 429:
                    raise RateLimitError("Guest token activation rate-limited")
                if resp.status_code >= 400:
                    last_error = f"{endpoint} -> HTTP {resp.status_code}"
                    continue

                data = resp.json()
                guest_token = data.get("guest_token")
                if guest_token:
                    return str(guest_token)
            except RateLimitError:
                raise
            except RequestsError as e:
                last_error = str(e)
            except Exception as e:
                last_error = str(e)

        message = f"Failed to activate guest token: {last_error}"
        if isinstance(last_error, str) and any(
            marker in last_error.lower()
            for marker in ("resolve", "timed out", "timeout", "connect")
        ):
            raise TransientNetworkError(message)
        raise ScraperError(message)

    async def _load_openapi_query_config(self, session: AsyncSession) -> None:
        if self._query_id:
            return
        try:
            resp = await session.get(self.OPENAPI_PLACEHOLDER_URL, timeout=20)
            if resp.status_code >= 400:
                logger.warning("OpenAPI placeholder fetch failed: HTTP %s", resp.status_code)
                return
            data = resp.json()
            entry = data.get("TweetResultByRestId") or {}
            query_id = entry.get("queryId")
            if isinstance(query_id, str) and query_id:
                self._query_id = query_id
            if isinstance(entry.get("variables"), dict):
                self._variables = dict(entry["variables"])
            if isinstance(entry.get("features"), dict):
                self._features = dict(entry["features"])
            if isinstance(entry.get("fieldToggles"), dict):
                self._field_toggles = dict(entry["fieldToggles"])
        except Exception as e:
            logger.warning("OpenAPI config load failed, using defaults: %s", e)

    def _build_graphql_url(self, query_id: str, tweet_id: str) -> str:
        variables = dict(self._variables)
        variables["tweetId"] = str(tweet_id)

        url = (
            f"https://x.com/i/api/graphql/{query_id}/TweetResultByRestId"
            f"?variables={urllib.parse.quote(json.dumps(variables, separators=(',', ':')))}"
            f"&features={urllib.parse.quote(json.dumps(self._features, separators=(',', ':')))}"
        )

        if self._field_toggles:
            url += (
                "&fieldToggles="
                + urllib.parse.quote(json.dumps(self._field_toggles, separators=(",", ":")))
            )

        return url

    async def _graphql_tweet_request(self, tweet_id: str) -> dict:
        async with AsyncSession(impersonate="chrome124") as session:
            # 1. Hit x.com to get initial cookies (ct0 / CSRF)
            try:
                await session.get("https://x.com", timeout=15)
            except Exception as e:
                logger.warning("Initial x.com hit failed: %s", e)

            await self._load_openapi_query_config(session)
            guest_token = await self._activate_guest_token(session)

            csrf_token = session.cookies.get("ct0")
            headers = {
                "Authorization": f"Bearer {self.BEARER_TOKEN}",
                "X-Guest-Token": guest_token,
                "X-Twitter-Active-User": "yes",
                "X-Twitter-Client-Language": "en",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://x.com/",
            }
            if csrf_token:
                headers["X-Csrf-Token"] = csrf_token

            query_ids: list[str] = []
            if self._query_id:
                query_ids.append(self._query_id)
            for qid in self.FALLBACK_QUERY_IDS:
                if qid not in query_ids:
                    query_ids.append(qid)

            last_error = None
            for query_id in query_ids:
                try:
                    url = self._build_graphql_url(query_id, tweet_id)
                    resp = await session.get(url, headers=headers, timeout=20)

                    if resp.status_code == 429:
                        raise RateLimitError("Twitter GraphQL rate-limited")
                    if resp.status_code in (401, 403):
                        last_error = f"HTTP {resp.status_code}"
                        continue
                    if resp.status_code >= 400:
                        last_error = f"HTTP {resp.status_code}"
                        continue

                    data = resp.json()
                    errors = data.get("errors") or []
                    if errors:
                        # Usually stale queryId / invalid variable combination.
                        last_error = str(errors[0])
                        continue

                    if data.get("data", {}).get("tweetResult") is None:
                        last_error = "Missing tweetResult"
                        continue

                    return data
                except RateLimitError:
                    raise
                except RequestsError as e:
                    raise TransientNetworkError(
                        f"Twitter GraphQL network request failed: {e}"
                    ) from e
                except Exception as e:
                    last_error = str(e)

            raise ScraperError(f"GraphQL TweetResultByRestId failed: {last_error}")

    # ------------------------------------------------------------------
    # Fallback provider
    # ------------------------------------------------------------------

    async def _fxtwitter_request(self, tweet_id: str) -> dict:
        url = f"https://api.fxtwitter.com/status/{tweet_id}"
        async with AsyncSession(impersonate="chrome124") as session:
            try:
                resp = await session.get(url, timeout=20)
            except RequestsError as e:
                raise TransientNetworkError(
                    f"FxTwitter network request failed: {e}"
                ) from e
            if resp.status_code == 429:
                raise RateLimitError("FxTwitter fallback rate-limited")
            if resp.status_code >= 500:
                raise TransientNetworkError(
                    f"FxTwitter fallback unavailable: HTTP {resp.status_code}"
                )
            if resp.status_code >= 400:
                raise NotFoundError(f"Fallback tweet not found: HTTP {resp.status_code}")
            data = resp.json()
            if data.get("code") != 200 or not data.get("tweet"):
                raise NotFoundError("Fallback provider returned no tweet")
            return data

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    @staticmethod
    def _unwrap_tweet_result(result: dict) -> dict:
        if not isinstance(result, dict):
            return {}
        cur = result
        for _ in range(3):
            typename = cur.get("__typename")
            if typename == "TweetWithVisibilityResults":
                cur = cur.get("tweet") or {}
                continue
            break
        return cur

    @staticmethod
    def _pick_best_video_variant(variants: list[dict]) -> Optional[str]:
        mp4 = [v for v in variants if (v.get("content_type") == "video/mp4" and v.get("url"))]
        if mp4:
            mp4.sort(key=lambda v: int(v.get("bitrate") or 0), reverse=True)
            return mp4[0].get("url")
        for v in variants:
            if v.get("url"):
                return v.get("url")
        return None

    @classmethod
    def _parse_legacy_media(cls, legacy: dict) -> list[dict]:
        media_nodes = (legacy.get("extended_entities") or {}).get("media") or []
        if not media_nodes:
            media_nodes = (legacy.get("entities") or {}).get("media") or []

        items: list[dict] = []
        seen = set()

        for node in media_nodes:
            media_type = node.get("type")
            original = node.get("original_info") or {}
            width = original.get("width")
            height = original.get("height")

            if media_type == "photo":
                url = node.get("media_url_https") or node.get("media_url")
                if url and url not in seen:
                    seen.add(url)
                    items.append(
                        {
                            "type": "image",
                            "url": url,
                            "width": width,
                            "height": height,
                        }
                    )
                continue

            if media_type in ("video", "animated_gif"):
                variants = (node.get("video_info") or {}).get("variants") or []
                video_url = cls._pick_best_video_variant(variants)
                if video_url and video_url not in seen:
                    seen.add(video_url)
                    items.append(
                        {
                            "type": "video",
                            "url": video_url,
                            "thumbnail": node.get("media_url_https") or node.get("media_url"),
                            "width": width,
                            "height": height,
                            "looping": media_type == "animated_gif",
                        }
                    )
                continue

        return items

    def _parse_graphql_tweet(self, payload: dict, tweet_id: str) -> dict:
        result = payload.get("data", {}).get("tweetResult", {}).get("result", {})
        return self._parse_graphql_result(result, tweet_id, include_quote=True)

    def _parse_graphql_result(
        self,
        result: dict,
        tweet_id: str,
        *,
        include_quote: bool,
    ) -> dict:
        result = self._unwrap_tweet_result(result)

        if not result:
            raise NotFoundError(f"Tweet {tweet_id} not found")

        typename = result.get("__typename")
        if typename == "TweetUnavailable":
            raise NotFoundError(f"Tweet {tweet_id} unavailable")

        legacy = result.get("legacy") or {}
        if not legacy:
            # A present Tweet object with an unexpected schema is not proof that
            # the tweet is gone. Treat it as a parser failure so scrape_tweet()
            # can continue to the FxTwitter fallback instead of returning 404.
            raise ScraperError(f"Tweet {tweet_id} missing legacy payload")

        user_result = result.get("core", {}).get("user_results", {}).get("result", {})
        user_legacy = user_result.get("legacy") or {}
        user_core = user_result.get("core") or {}

        # X moved identity fields from User.legacy to User.core in July 2026.
        # Keep the legacy fallback for older responses and FxTwitter fixtures.
        username = (
            user_core.get("screen_name")
            or user_legacy.get("screen_name")
            or "unknown"
        )
        author_name = user_core.get("name") or user_legacy.get("name")
        avatar_url = (
            (user_result.get("avatar") or {}).get("image_url")
            or user_legacy.get("profile_image_url_https")
            or user_legacy.get("profile_image_url")
        )
        note_tweet_result = (
            ((result.get("note_tweet") or {}).get("note_tweet_results") or {}).get("result")
            or {}
        )
        # For long-form posts, legacy.full_text is only a preview ending in a
        # t.co continuation/media link. The Note Tweet text contains the whole
        # post (including the preview), so it must replace rather than be
        # appended to the legacy value.
        text = note_tweet_result.get("text") or legacy.get("full_text") or ""

        text_entities = note_tweet_result.get("entity_set") or legacy.get("entities") or {}
        entities_urls = text_entities.get("urls") or []
        expanded_link = None
        for u in entities_urls:
            url_short = u.get("url")
            expanded = u.get("expanded_url")
            if expanded:
                if not expanded_link:
                    expanded_link = expanded
                if url_short and text:
                    text = text.replace(url_short, expanded)

        views = result.get("views", {})
        media = self._parse_legacy_media(legacy)

        # Strip t.co media links from text
        media_entities = (legacy.get("extended_entities") or {}).get("media") or []
        if not media_entities:
            media_entities = (legacy.get("entities") or {}).get("media") or []
        for m in media_entities:
            m_url = m.get("url")
            if m_url and text:
                text = text.replace(m_url, "").strip()

        parsed = {
            "tweet_id": str(result.get("rest_id") or legacy.get("id_str") or tweet_id),
            "url": f"https://x.com/{username}/status/{result.get('rest_id') or tweet_id}",
            "text": text,
            "created_at": legacy.get("created_at"),
            "username": username,
            "author_name": author_name,
            "avatar_url": avatar_url,
            "like_count": legacy.get("favorite_count"),
            "reply_count": legacy.get("reply_count"),
            "repost_count": legacy.get("retweet_count"),
            "quote_count": legacy.get("quote_count"),
            "view_count": int(views.get("count")) if str(views.get("count", "")).isdigit() else None,
            "link": expanded_link,
            "nsfw": bool(legacy.get("possibly_sensitive") or legacy.get("sensitive_media_warning")),
            "media": media,
            "source": "graphql",
        }
        if include_quote:
            quoted_result = (result.get("quoted_status_result") or {}).get("result")
            quote = None
            if isinstance(quoted_result, dict) and quoted_result:
                try:
                    quote = self._parse_graphql_result(
                        quoted_result,
                        str(legacy.get("quoted_status_id_str") or "quote"),
                        include_quote=False,
                    )
                except (NotFoundError, ScraperError) as exc:
                    logger.info("Quoted tweet is unavailable: %s", exc)
            parsed["quote"] = quote
        return parsed

    @classmethod
    def _parse_fxtwitter_media(cls, tweet_obj: dict) -> list[dict]:
        media_all = (tweet_obj.get("media") or {}).get("all") or []
        items: list[dict] = []
        seen = set()

        for m in media_all:
            mtype = m.get("type")
            if mtype in ("video", "gif"):
                variants = m.get("variants") or []
                video_url = cls._pick_best_video_variant(variants)
                if not video_url:
                    video_url = m.get("url")
                if video_url and video_url not in seen:
                    seen.add(video_url)
                    items.append(
                        {
                            "type": "video",
                            "url": video_url,
                            "thumbnail": m.get("thumbnail_url"),
                            "width": m.get("width"),
                            "height": m.get("height"),
                            "looping": mtype == "gif",
                        }
                    )
            else:
                img_url = m.get("url")
                if img_url and img_url not in seen:
                    seen.add(img_url)
                    items.append(
                        {
                            "type": "image",
                            "url": img_url,
                            "width": m.get("width"),
                            "height": m.get("height"),
                        }
                    )

        return items

    def _parse_fxtwitter_tweet(self, payload: dict, tweet_id: str) -> dict:
        tweet = payload.get("tweet") or {}
        if not tweet:
            raise NotFoundError(f"Tweet {tweet_id} not found")

        return self._parse_fxtwitter_object(tweet, tweet_id, include_quote=True)

    def _parse_fxtwitter_object(
        self,
        tweet: dict,
        tweet_id: str,
        *,
        include_quote: bool,
    ) -> dict:

        author = tweet.get("author") or {}
        username = author.get("screen_name") or author.get("name") or "unknown"

        parsed = {
            "tweet_id": str(tweet.get("id") or tweet_id),
            "url": tweet.get("url") or f"https://x.com/{username}/status/{tweet_id}",
            "text": tweet.get("text") or tweet.get("raw_text") or "",
            "created_at": tweet.get("created_at") or tweet.get("created_timestamp"),
            "username": username,
            "author_name": author.get("name"),
            "avatar_url": author.get("avatar_url") or author.get("avatar"),
            "like_count": tweet.get("likes"),
            "reply_count": tweet.get("replies"),
            "repost_count": tweet.get("retweets"),
            "quote_count": tweet.get("quotes"),
            "view_count": tweet.get("views"),
            "link": None,
            "nsfw": bool(tweet.get("possibly_sensitive")),
            "media": self._parse_fxtwitter_media(tweet),
            "source": "fxtwitter",
        }
        if include_quote:
            quoted = tweet.get("quote")
            parsed["quote"] = (
                self._parse_fxtwitter_object(
                    quoted,
                    str(quoted.get("id") or "quote"),
                    include_quote=False,
                )
                if isinstance(quoted, dict) and quoted
                else None
            )
        return parsed
