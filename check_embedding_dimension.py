import sys
from service.embed_vertex import embed_text, MODEL_ID

try:
    embedding = embed_text("healthcheck")
    dimension = len(embedding)
    print(f"Embedding dimension for model '{MODEL_ID}': {dimension}")
    assert dimension == 768
    print("Dimension check passed.")
except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
    sys.exit(1)