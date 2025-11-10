# builder stage
FROM python:3.9-slim AS builder

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Also install test dependencies to run tests
RUN pip install pytest pytest-cov pandera

# Copy tests
COPY tests/ ./tests/

# final stage
FROM python:3.9-slim

# Set provenance arguments
ARG GIT_SHA
ARG IMAGE_DIGEST

# Set the working directory in the container
WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
# Explicitly copy uvicorn executable from builder stage
# COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn # Removed as we are using python -m uvicorn

# Set environment variables for provenance
ENV GIT_SHA=$GIT_SHA
ENV IMAGE_DIGEST=$IMAGE_DIGEST

# Set the port the application will run on
ENV PORT 8080

# Command to run the FastAPI application
CMD ["python", "-m", "uvicorn", "service.search_api:app", "--host", "0.0.0.0", "--port", "8080"]
