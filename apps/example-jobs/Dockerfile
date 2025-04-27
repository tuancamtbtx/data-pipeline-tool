# this Dockerfile should be built from the repo root
FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=True
WORKDIR /workspace

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

COPY pyproject.toml uv.lock /workspace/
RUN uv sync --frozen --no-install-workspace --package=sophia-service

COPY . /workspace
RUN uv sync --frozen --package=sophia-service

ENV PATH="/workspace/.venv/bin:$PATH"

ENTRYPOINT [ "uv", "run", "/workspace/apps/sophia-service/main.py" ]