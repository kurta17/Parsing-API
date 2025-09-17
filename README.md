# Orders API Clients

Two clients to fetch 1000 orders from API (`/item/{id}`), write to CSV.

**Threaded client:** `client_threads.py` → `items_threads.csv`
**Async client:** `client_async.py` → `items_async.csv`

## Usage

1. Install deps: `uv sync`
2. Run: `uv run client_threads.py` or `uv run client_async.py`


## Constants used

- `TOTAL_ITEMS = 1000` — number of orders to fetch and write
- `RATE_LIMIT = 18` — max requests per second (to avoid server overload)
- `MAX_WORKERS = 10` (threads) / `MAX_CONCURRENT = 50` (async) — controls parallelism
- `RETRY_ATTEMPTS = 5` — how many times to retry on error

## Explanation

Both clients download up to 1000 orders from the API, handling errors and rate limits. If the server is down or slow, the client will retry up to 5 times per order. Results are saved in a CSV file with columns for each order field.


3. Run the client:
   ```sh
   uv run client_threads.py
   ```
4. Output will be in `items_threads.csv`.

