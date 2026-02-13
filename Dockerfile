FROM python:3.11-slim

# Create a non-root user
RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app
ENV SQLITE_DB_PATH=/tmp/processed.db

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Optional: document the port
EXPOSE 5000

# Run Gunicorn with a single worker
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers=1", "app:app"]

