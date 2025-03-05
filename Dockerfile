# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scripts (API + data processing)
COPY scripts /app/scripts

# Copy the data folder (will be mounted as a volume)
COPY data /app/data

# Expose port 8000 for the API
EXPOSE 8000

# Default command to start the FastAPI API
CMD ["uvicorn", "scripts.api.app:app", "--host", "0.0.0.0", "--port", "8000"]