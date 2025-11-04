# Dockerfile
FROM prefecthq/prefect:3-latest  # or prefecthq/prefect:2-latest for Prefect 2.x

# Set work directory
WORKDIR /opt/prefect

# Copy flow code into the image
COPY flows/ ./flows/

# Default command (overridden by docker-compose)
CMD ["prefect", "agent", "start", "-q", "default"]

