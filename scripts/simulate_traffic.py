import argparse
import random
import time
import requests
import logging
import xml.etree.ElementTree as ET
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ARXIV_API_URL = "https://export.arxiv.org/api/query"

def get_random_arxiv_papers(num_papers: int = 50, category: str = "cs.AI") -> List[Dict]:
    """
    Fetches a list of recent papers from arXiv to use for simulation.
    """
    logging.info(f"Fetching up to {num_papers} recent papers from arXiv in category '{category}'...")
    params = {
        "search_query": f"cat:{category}",
        "start": 0,
        "max_results": num_papers,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    try:
        response = requests.get(ARXIV_API_URL, params=params, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        papers = []
        # Atom feed namespace
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        for entry in root.findall('atom:entry', ns):
            id_tag = entry.find('atom:id', ns)
            if id_tag is not None:
                # The ID tag contains the abstract URL
                papers.append({'url': id_tag.text})

        if not papers:
            logging.error("Could not parse any papers from arXiv response.")
            return []

        logging.info(f"Successfully fetched {len(papers)} paper URLs from arXiv.")
        return papers
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch papers from arXiv: {e}")
        return []
    except ET.ParseError as e:
        logging.error(f"Failed to parse arXiv XML response: {e}")
        return []


def simulate_traffic(service_url: str, num_requests: int, k: int):
    """
    Simulates traffic by sending requests to the search service.
    """
    if not service_url.startswith("http"):
        service_url = f"https://{service_url}"
    search_endpoint = f"{service_url.rstrip('/')}/search"

    papers = get_random_arxiv_papers(num_requests)
    if not papers:
        logging.error("Halting simulation due to failure in fetching papers.")
        return

    logging.info(f"Starting simulation with {num_requests} requests to {search_endpoint}...")

    successful_requests = 0
    for i in range(num_requests):
        paper = random.choice(papers)
        payload = {
            "url": paper['url'],
            "k": k
        }

        try:
            logging.info(f"Request {i+1}/{num_requests}: POSTing URL {payload['url']}")
            response = requests.post(search_endpoint, json=payload, timeout=60)

            if response.status_code == 200:
                neighbors = response.json().get("neighbors", [])
                first_neighbor_id = "N/A"
                if neighbors:
                    first_neighbor_id = neighbors[0].get("id", "N/A")
                logging.info(f"  -> Success ({response.status_code}). Top result: {first_neighbor_id}")
                successful_requests += 1
            else:
                logging.warning(f"  -> Failed ({response.status_code}). Response: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"  -> Exception during request: {e}")

        # Add a small random delay between requests
        time.sleep(random.uniform(0.5, 2.0))

    logging.info(f"Simulation complete. {successful_requests}/{num_requests} requests were successful.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate traffic to the SciPaper-Hub search service.")
    parser.add_argument(
        "service_url",
        help="The base URL of the deployed search service (e.g., paperrec-search-XXXX-uc.a.run.app)."
    )
    parser.add_argument(
        "--num-requests",
        type=int,
        default=50,
        help="The number of search requests to simulate."
    )
    parser.add_argument(
        "--k",
        type=int,
        default=5,
        help="The number of neighbors to request."
    )
    args = parser.parse_args()

    simulate_traffic(service_url=args.service_url, num_requests=args.num_requests, k=args.k)
