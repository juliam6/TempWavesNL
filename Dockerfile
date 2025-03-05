FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY scripts /app/scripts

# Expose API port
EXPOSE 8000

# Start API by default
CMD ["uvicorn", "scripts.api.app:app", "--host", "0.0.0.0", "--port", "8000"]