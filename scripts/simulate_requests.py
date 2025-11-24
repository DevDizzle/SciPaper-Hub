import requests
import json
import time
import random

SEARCH_ENDPOINT = "https://paperrec-search-550651297425.us-central1.run.app/search"
ARXIV_URLS = [
    "https://arxiv.org/abs/2511.08747",
    "https://arxiv.org/abs/2511.08715",
    "https://arxiv.org/abs/2511.09558",
    "https://arxiv.org/abs/2511.09533",
    "https://arxiv.org/abs/2511.09515",
    "https://arxiv.org/abs/2511.09478",
    "https://arxiv.org/abs/2511.09396",
    "https://arxiv.org/abs/2511.09381",
    "https://arxiv.org/abs/2511.09443",
    "https://arxiv.org/abs/2511.09404",
    "https://arxiv.org/abs/2511.09425",
    "https://arxiv.org/abs/2511.09416",
    "https://arxiv.org/abs/2511.09414",
    "https://arxiv.org/abs/2511.09392",
    "https://arxiv.org/abs/2511.09397",
    "https://arxiv.org/abs/2511.09388",
    "https://arxiv.org/abs/2511.09352",
    "https://arxiv.org/abs/2511.09298",
    "https://arxiv.org/abs/2511.09286",
    "https://arxiv.org/abs/2511.09082",
]

NUM_REQUESTS = 100
K_VALUE = 5
DELAY_SECONDS = 0.1 # Small delay to simulate user behavior

def run_simulation():
    print(f"Starting simulation: Sending {NUM_REQUESTS} requests to {SEARCH_ENDPOINT}")
    successful_requests = 0
    for i in range(NUM_REQUESTS):
        url_to_search = random.choice(ARXIV_URLS) # Randomly pick a URL
        payload = {"url": url_to_search, "k": K_VALUE}
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(SEARCH_ENDPOINT, headers=headers, data=json.dumps(payload))
            response.raise_for_status() # Raise an exception for HTTP errors
            print(f"Request {i+1}/{NUM_REQUESTS} successful for {url_to_search}. Status: {response.status_code}")
            successful_requests += 1
        except requests.exceptions.RequestException as e:
            print(f"Request {i+1}/{NUM_REQUESTS} failed for {url_to_search}. Error: {e}")
        
        time.sleep(DELAY_SECONDS)

    print(f"Simulation finished. Successful requests: {successful_requests}/{NUM_REQUESTS}")

if __name__ == "__main__":
    run_simulation()
