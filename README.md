# Pinchana Twitter/X

This FastAPI module extracts public posts from X/Twitter and supported fixup domains. It uses X GraphQL with guest-token and CSRF handling, and can use the FxTwitter API as a metadata fallback. Confirmed rate limits and repeated DNS/connectivity failures trigger a bounded retry with Gluetun recovery; exhausted transient failures remain typed `503` responses.

## Result behavior

- Images and videos are downloaded into `/media/twitter/{post_id}/...` within the shared cache.
- Animated GIF-style posts are returned as video assets with `looping: true` in the gateway's API v1 response.
- A client that requires an actual GIF must perform the conversion explicitly. Pinchana Web exposes this as the **Convert Twitter GIFs** setting.

## API

- `POST /scrape` accepts `{"url":"https://x.com/account/status/POST_ID"}`.
- `GET /media/twitter/{post_id}/{filename}` serves cached media inside the trusted gateway network.
- `GET /health` reports service and VPN readiness.

External clients should use the gateway's authenticated `POST /v1/scrape` and `/media/...` routes.

## Development

```sh
uv sync --frozen
uv run uvicorn pinchana_twitter.main:app --host 0.0.0.0 --port 8089 --reload
```

```sh
# Run from the parent pinchana-api directory.
docker build --file pinchana-twitter/Dockerfile --tag pinchana-twitter:local .
```
