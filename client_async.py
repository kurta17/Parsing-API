import httpx
import asyncio
import csv
import logging
from typing import Dict, Any, Optional
from aiolimiter import AsyncLimiter
import aiofiles

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Constants
ENDPOINT = 'http://127.0.0.1:8000/item/{}'
RATE_LIMIT = 18  # requests per second
TOTAL_ITEMS = 1000
CSV_FILE = 'items_async.csv'
RETRY_ATTEMPTS = 5
MAX_CONCURRENT = 50  # semaphore limit for burst concurrency

async def fetch_order(order_id: int, client: httpx.AsyncClient, limiter: AsyncLimiter, semaphore: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
    """Fetch a single order with rate limiting and retries"""
    url = ENDPOINT.format(order_id)
    attempts = 0
    
    async with semaphore:  # Limit concurrent requests
        while attempts < RETRY_ATTEMPTS:
            try:
                async with limiter:  # Rate limiting
                    response = await client.get(url, timeout=2.0)
                    
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
                    logger.warning(f"429 too many requests for order {order_id}, retrying after {retry_after}s (attempt {attempts+1})")
                    await asyncio.sleep(retry_after)
                elif 500 <= response.status_code < 600:
                    logger.warning(f"{response.status_code} server error for order {order_id}, retrying after 1s (attempt {attempts+1})")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"error {response.status_code} for order {order_id}")
                    return None
                    
            except (httpx.TimeoutException, httpx.TransportError) as e:
                logger.warning(f"transport/timeout error for order {order_id}: {e}, retrying (attempt {attempts+1})")
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"unexpected error for order {order_id}: {e}, retrying (attempt {attempts+1})")
                await asyncio.sleep(1)
                
            attempts += 1
            
        logger.error(f"failed to fetch order {order_id} after {RETRY_ATTEMPTS} attempts")
        return None

async def write_to_csv_async(orders: list, filename: str):
    """Write orders to CSV asynchronously using aiofiles"""
    async with aiofiles.open(filename, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['order_id', 'account_id', 'company', 'status', 'currency', 'subtotal', 'tax', 'total', 'created_at']
        
        # Write header
        header = ','.join(fieldnames) + '\n'
        await f.write(header)
        
        # Write data rows
        for order in orders:
            row_values = [str(order.get(field, '')) for field in fieldnames]
            row = ','.join(f'"{val}"' if ',' in str(val) else str(val) for val in row_values) + '\n'
            await f.write(row)

async def main():
    """Main async function"""
    limiter = AsyncLimiter(RATE_LIMIT, 1)  # 18 requests per second
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)  # Limit concurrent requests
    
    async with httpx.AsyncClient() as client:
        tasks = []
        results = []
        
        # Create tasks for fetching orders
        for order_id in range(1, TOTAL_ITEMS + 1):
            task = asyncio.create_task(fetch_order(order_id, client, limiter, semaphore))
            tasks.append(task)
        
        # Process completed tasks as they finish
        for task in asyncio.as_completed(tasks):
            try:
                order = await task
                if order:
                    results.append(order)
                    if len(results) % 100 == 0:  # Progress logging
                        logger.info(f"fetched {len(results)} orders so far...")
                        
                # Stop early if we have enough results
                if len(results) >= TOTAL_ITEMS:
                    # Cancel remaining tasks
                    for remaining_task in tasks:
                        if not remaining_task.done():
                            remaining_task.cancel()
                    break
                    
            except asyncio.CancelledError:
                continue
            except Exception as e:
                logger.error(f"unexpected error processing task: {e}")
    
    # Write results to CSV
    if results:
        await write_to_csv_async(results[:TOTAL_ITEMS], CSV_FILE)
        logger.info(f"wrote {len(results[:TOTAL_ITEMS])} orders to {CSV_FILE}")
    else:
        logger.error("no successful orders fetched")

if __name__ == '__main__':
    asyncio.run(main())
