FROM python:3.11-slim

# Create a non-root user
RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

# Create persistent job data directory (matches docker-compose volume)
RUN mkdir -p /var/lib/myapp && chown -R appuser:appuser /var/lib/myapp

ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Run with Gunicorn (1 worker so APScheduler doesn't duplicate jobs)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers=1", "app:app"]

