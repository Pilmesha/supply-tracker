FROM python:3.11-slim

# Create a non-root user
RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app

ENV SQLITE_DB_PATH=/tmp/processed.db
ENV PYTHONUNBUFFERED=1
ENV TMPDIR=/tmp

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app with proper ownership (ONLY ONCE)
COPY --chown=appuser:appuser . .

# Switch to non-root user
USER appuser

EXPOSE 5000

CMD ["gunicorn", "--worker-tmp-dir", "/tmp", "--bind", "0.0.0.0:5000", "--workers=1","app:app"]
