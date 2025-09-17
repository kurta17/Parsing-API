import httpx
import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from ratelimit import limits, sleep_and_retry
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# constant
ENDPOINT = 'http://127.0.0.1:8000/item/{}'
MAX_WORKERS = 10
RATE_LIMIT = 18
TOTAL_ITEMS = 1000
CSV_FILE = 'items_threads.csv'
RETRY_ATTEMPTS = 5


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=1)
def limited_request():
    pass 

def fetch_order(order_id: int, client: httpx.Client) -> Dict[str, Any]:
    url = ENDPOINT.format(order_id)
    attempts = 0
    while attempts < RETRY_ATTEMPTS:
        try:
            limited_request()
            response = client.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    'order_id': data.get('order_id'),
                    'account_id': data.get('account_id'),
                    'company': data.get('company'),
                    'status': data.get('status'),
                    'currency': data.get('currency'),
                    'subtotal': data.get('subtotal'),
                    'tax': data.get('tax'),
                    'total': data.get('total'),
                    'created_at': data.get('created_at'),
                }
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', '1'))
                logger.warning(f"429 too many Requests for order {order_id}, retrying after {retry_after} (attempt {attempts+1})")
                time.sleep(retry_after)
            elif 500 <= response.status_code < 600:
                logger.warning(f"{response.status_code} server rrror for order {order_id},(attempt {attempts+1})")
                time.sleep(1)
            else:
                logger.error(f"error {response.status_code} for order {order_id}")
                return None
        except (httpx.TimeoutException, httpx.TransportError) as e:
            logger.warning(f"transport/timeout error for order {order_id}: {e}, retrying (attempt {attempts+1})")
            time.sleep(1)
        attempts += 1
    logger.error(f"failed to fetch order {order_id} after {RETRY_ATTEMPTS} attempts")
    return None

def main():
    with httpx.Client() as client, open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['order_id', 'account_id', 'company', 'status', 'currency', 'subtotal', 'tax', 'total', 'created_at']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        results = []
        next_id = 1
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = set()
            while len(results) < TOTAL_ITEMS:
                while len(futures) < MAX_WORKERS and next_id <= 10_000_000:
                    futures.add(executor.submit(fetch_order, next_id, client))
                    next_id += 1
                done, futures = set(), futures
                for future in as_completed(futures, timeout=10):
                    try:
                        order = future.result()
                        if order:
                            results.append(order)
                            writer.writerow(order)
                            if len(results) >= TOTAL_ITEMS:
                                break
                    except Exception as e:
                        logger.error(f"Exception in future: {e}")
                    done.add(future)
                futures -= done
    logger.info(f"write {len(results)} orders to {CSV_FILE}")

if __name__ == '__main__':
    main()
