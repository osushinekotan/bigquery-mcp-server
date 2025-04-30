FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


COPY pyproject.toml /app/
RUN uv pip install -r ./pyproject.toml --system

COPY . /app/

CMD ["python", "main.py"]