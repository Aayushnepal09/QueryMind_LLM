FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1

RUN pip install --no-cache-dir uv

WORKDIR /app

# Dependency layer first so source edits do not invalidate the install cache.
COPY pyproject.toml uv.lock README.md ./
RUN uv pip install --system -r pyproject.toml

COPY src/ ./src/
COPY app/ ./app/
COPY third_party/ ./third_party/
COPY results/splits.json ./results/splits.json
RUN uv pip install --system --no-deps -e .

# data/ is mounted at runtime: BIRD is 1.4 GB and must not live in the image.
VOLUME ["/app/data"]

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "sqlsentinel.api:app", "--host", "0.0.0.0", "--port", "8000"]
