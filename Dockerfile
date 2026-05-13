FROM python:3.13-slim

WORKDIR /workspace/pinchana-twitter

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy pinchana-core (local path dependency) first
COPY pinchana-core/pyproject.toml pinchana-core/uv.lock pinchana-core/README.md ../pinchana-core/
RUN mkdir -p ../pinchana-core/src
COPY pinchana-core/src ../pinchana-core/src

# Copy scraper package files
COPY pinchana-twitter/pyproject.toml pinchana-twitter/uv.lock pinchana-twitter/README.md ./
RUN uv sync --frozen --no-install-project

COPY pinchana-twitter/src ./src
RUN uv sync --frozen

RUN mkdir -p /app/cache
ENV CACHE_PATH=/app/cache
ENV CACHE_MAX_SIZE_GB=10.0

EXPOSE 8089
CMD ["uv", "run", "uvicorn", "pinchana_twitter.main:app", "--host", "0.0.0.0", "--port", "8089"]
