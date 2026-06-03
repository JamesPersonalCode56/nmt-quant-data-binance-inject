FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false

RUN pip install --no-cache-dir poetry

WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

COPY . .

# default: live collection. Override for migrate/backfill:
#   docker compose run --rm crawler python migrate.py
#   docker compose run --rm crawler python -m backfill.main
CMD ["python", "-m", "live.main"]
