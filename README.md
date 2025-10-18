# SciPaper Hub

SciPaper Hub provides two major components:

* **Similarity Service** – A FastAPI application that exposes `POST /search`
  and returns the top-k similar arXiv papers for a supplied URL by querying a
  Vertex Vector Search collection.
* **Nightly Pipelines** – Harvest, normalize, and index arXiv metadata so the
  service can respond quickly using a fresh embedding index.

## Getting started

### Requirements

* Python 3.10+
* Access to Google Cloud APIs (Vertex AI, Cloud Storage, Secret Manager if
  required by your environment)
* The following environment variables configured: `PROJECT_ID`, `REGION`,
  `DATA_BUCKET`, `VECTOR_COLLECTION_ID`, and optionally `VERTEX_LOCATION` if it
  differs from `REGION`.

Install dependencies:

```bash
pip install -r requirements.txt
```

### Running the similarity API

```bash
uvicorn service.search_api:app --host 0.0.0.0 --port 8080
```

Send a request:

```bash
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{"url": "https://arxiv.org/abs/1706.03762", "k": 5}'
```

### Nightly data pipelines

1. **Harvest** – Fetches all arXiv Atom entries submitted in the previous UTC
   day for the target CS categories and saves the raw Atom feeds + manifest to
   Cloud Storage.
   ```bash
   python -m pipelines.harvest
   ```
2. **Normalize** – Deduplicates and transforms the Atom XML into a Parquet file
   with rich metadata, one record per `base_id`.
   ```bash
   python -m pipelines.normalize 20240101T000000Z
   ```
3. **Indexer** – Embeds abstracts with Vertex `gemini-embedding-001` and
   upserts them into the Vertex Vector Search collection, validating the write
   with read-after-write probes.
   ```bash
   python -m pipelines.indexer 20240101T000000Z
   ```

Each step accepts optional flags to override snapshot identifiers or blob
locations; see the module docstrings for details.

## Development notes

* The arXiv client enforces a 3 second delay between requests to stay within
  the published API guidelines.
* Embeddings are cached in memory per process keyed by the text digest to avoid
  redundant Vertex AI calls.
* Vector metadata stores serialized JSON for structured fields (authors,
  categories) to support detailed responses in the `/search` API.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
