FROM python:3.11-slim

# Create a non-root user
RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY . .

# Switch to non-root
USER appuser

ENV PYTHONUNBUFFERED=1

# Run with Gunicorn (secure Flask server)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]

