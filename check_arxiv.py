import sys
from service import arxiv_client

try:
    arxiv_id = arxiv_client.parse_arxiv_url("https://arxiv.org/abs/2510.04618")
    if not arxiv_id:
        print("Could not parse arXiv ID from URL.", file=sys.stderr)
        sys.exit(1)
    entry = arxiv_client.get_by_id(arxiv_id)
    print(entry["abstract"])
except arxiv_client.ArxivNotFoundError:
    print(f"ArXiv paper with ID {arxiv_id} not found.", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
    sys.exit(1)
