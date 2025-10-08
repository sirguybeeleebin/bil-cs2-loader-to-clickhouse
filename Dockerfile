FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s /root/.local/bin/poetry /usr/local/bin/poetry \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN poetry install --no-root --no-interaction --no-ansi

COPY . .

CMD ["poetry", "run", "python", "main.py", "--env-file", ".env"]
