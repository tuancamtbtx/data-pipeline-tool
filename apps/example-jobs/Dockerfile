# this Dockerfile should be built from the repo root
FROM python:3.13-slim-bookworm

ENV PYTHONUNBUFFERED=True
WORKDIR /workspace

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

COPY pyproject.toml uv.lock /workspace/
RUN uv sync --frozen --no-install-workspace --package=example-jobs

COPY . /workspace
RUN uv sync --frozen --package=example-jobs

ENV PATH="/workspace/.venv/bin:$PATH"

ENTRYPOINT [ "uv", "run", "/workspace/apps/example-jobs/main.py" ]