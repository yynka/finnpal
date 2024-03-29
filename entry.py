import os
import asyncio
import aiohttp
import json
import logging
from datetime import datetime, time, timedelta
import pytz

# Configure logging
logging.basicConfig(
    filename='rsi_script.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Load your Polygon API key
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

if not POLYGON_API_KEY:
    logger.error("Polygon API key not found in environment variables.")
    raise Exception("Polygon API key not found in environment variables.")

# Base URLs
TICKERS_URL = 'https://api.polygon.io/v3/reference/tickers'
RSI_URL_TEMPLATE = 'https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan=minute&adjusted=true&window=14&series_type=close&order=desc&limit=1&apiKey={api_key}'

# Timezone settings
EASTERN_TZ = pytz.timezone('US/Eastern')
MARKET_OPEN = time(4, 0, 0)    # 4:00 AM EST
MARKET_CLOSE = time(20, 0, 0)  # 8:00 PM EST

# Global variables
tickers_with_low_rsi = {}  # Dictionary to store tickers and their RSI values

async def get_all_tickers(session):
    """
    Fetch all active stock tickers.
    """
    tickers = []
    params = {
        'market': 'stocks',
        'active': 'true',
        'limit': 1000,
        'apiKey': POLYGON_API_KEY
    }
    next_url = TICKERS_URL
    while next_url:
        try:
            async with session.get(next_url, params=params) as response:
                data = await response.json()
                if 'results' in data:
                    tickers.extend([item['ticker'] for item in data['results']])
                # Handle pagination
                if 'next_url' in data and data['next_url']:
                    next_url = data['next_url']
                    params = {'apiKey': POLYGON_API_KEY}
                else:
                    next_url = None
        except Exception as e:
            logger.error(f"Error fetching tickers: {e}")
            break
    logger.info(f"Total tickers retrieved: {len(tickers)}")
    return tickers

async def get_rsi_for_ticker(session, ticker):
    """
    Asynchronously fetches the RSI for a given ticker.
    """
    url = RSI_URL_TEMPLATE.format(ticker=ticker, api_key=POLYGON_API_KEY)
    try:
        async with session.get(url) as response:
            data = await response.json()
            if 'results' in data and 'values' in data['results'] and data['results']['values']:
                rsi_value = data['results']['values'][0]['value']
                return ticker, rsi_value
            else:
                logger.warning(f"No RSI data for ticker {ticker}.")
                return ticker, None
    except Exception as e:
        logger.error(f"Error fetching RSI for {ticker}: {e}")
        return ticker, None

async def process_tickers(tickers):
    """
    Process all tickers to fetch RSI values concurrently.
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for ticker in tickers:
            tasks.append(get_rsi_for_ticker(session, ticker))

        results = await asyncio.gather(*tasks)

        new_tickers_with_low_rsi = {}
        for ticker, rsi in results:
            if rsi is not None and rsi < 40:
                new_tickers_with_low_rsi[ticker] = rsi

        return new_tickers_with_low_rsi

def update_tickers_list(new_tickers_with_low_rsi):
    """
    Update the global tickers_with_low_rsi dictionary based on the new data.
    """
    global tickers_with_low_rsi

    # Update existing tickers and add new ones
    for ticker in new_tickers_with_low_rsi:
        tickers_with_low_rsi[ticker] = new_tickers_with_low_rsi[ticker]

    # Remove tickers that are no longer in the new list
    tickers_to_remove = [ticker for ticker in tickers_with_low_rsi if ticker not in new_tickers_with_low_rsi]
    for ticker in tickers_to_remove:
        del tickers_with_low_rsi[ticker]

    # Save updated list to JSON file
    with open('tickers_with_rsi_below_40.json', 'w') as f:
        json.dump(tickers_with_low_rsi, f, indent=4)

    logger.info(f"Updated list of tickers with RSI below 40: {len(tickers_with_low_rsi)}")

def is_market_open():
    """
    Check if the current time is within market hours (4:00 AM to 8:00 PM EST).
    """
    now = datetime.now(EASTERN_TZ).time()
    return MARKET_OPEN <= now <= MARKET_CLOSE

async def main():
    """
    Main function to run the script continuously during market hours.
    """
    # Fetch all tickers once at the start
    async with aiohttp.ClientSession() as session:
        tickers = await get_all_tickers(session)

    while True:
        if is_market_open():
            start_time = time.time()
            logger.info("Starting RSI update cycle.")

            # Process tickers to get RSI values
            new_tickers_with_low_rsi = await process_tickers(tickers)

            # Update the global list
            update_tickers_list(new_tickers_with_low_rsi)

            elapsed_time = time.time() - start_time
            logger.info(f"RSI update cycle completed in {elapsed_time:.2f} seconds.")

            # Wait before the next update (adjust as needed)
            await asyncio.sleep(300)  # Wait for 5 minutes
        else:
            logger.info("Market is closed. Waiting for market to open.")
            # Wait until market opens
            now = datetime.now(EASTERN_TZ)
            next_market_open = datetime.combine(now.date(), MARKET_OPEN, tzinfo=EASTERN_TZ)
            if now.time() > MARKET_CLOSE:
                next_market_open += timedelta(days=1)
            sleep_seconds = (next_market_open - now).total_seconds()
            await asyncio.sleep(sleep_seconds)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"An error occurred in the main execution: {e}")
