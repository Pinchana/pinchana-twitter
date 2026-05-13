# pinchana-twitter

Twitter/X scraper module for Pinchana.

## Strategy

1. **Primary:** X internal GraphQL (`TweetResultByRestId`) using guest token activation + CSRF (`ct0`) handling.
2. **Resilience:** Automatic VPN IP rotation via Gluetun on rate limits (429/403).
3. **Fallback:** `api.fxtwitter.com` for public tweet metadata when GraphQL is blocked.
4. **Media:** Download media to local cache and expose via `/media/twitter/{tweet_id}/{file}`.

## API

- `POST /scrape` — scrape a tweet URL
- `GET /media/twitter/{tweet_id}/{filename}` — serve cached media
- `GET /health` — VPN status
