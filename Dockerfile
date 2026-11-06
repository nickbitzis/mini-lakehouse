# Batch image: runs the pipeline and exits. The orchestrator watches the exit code.
FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 lake
WORKDIR /app
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH" MINILAKEHOUSE_HOME=/data
RUN mkdir -p /data && chown lake:lake /data
USER lake
VOLUME ["/data"]
HEALTHCHECK --interval=30s --timeout=5s CMD ["minilakehouse", "ls"]
ENTRYPOINT ["minilakehouse"]
CMD ["run"]
