FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# psycopg[binary] ships a manylinux wheel for slim/x86_64+arm64, so no
# compiler/libpq-dev build step is required.
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY app ./app

EXPOSE 8000

# Two long-lived psycopg connections are created during FastAPI lifespan;
# the app also retries DB connect for up to DB_CONNECT_TIMEOUT seconds.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
