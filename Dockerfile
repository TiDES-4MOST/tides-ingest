# The flow-run image of the TiDES Prefect deployments, built by `prefect deploy` (prefect.yaml).
# Pinned so that the server, the worker and the flows run the same Prefect version (requirements.txt).
# Secrets are never in the image: .env is excluded (.dockerignore) and mounted at run time.
FROM prefecthq/prefect:3.6.11-python3.12

# Fixed path, independent of the name of the checkout directory; prefect.yaml's pull step uses it.
WORKDIR /opt/prefect/tides-ingest

COPY requirements.txt .
RUN uv pip install -r requirements.txt

COPY . .
