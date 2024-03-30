import os
import asyncio
import aiohttp
import json
import logging
import time
from datetime import datetime, timedelta, time as datetime_time
import pytz
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_scanner.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

logger.debug(f"API Key loaded: {POLYGON_API_KEY[:5]}...{POLYGON_API_KEY[-5:] if POLYGON_API_KEY else None}")

if not POLYGON_API_KEY:
    logger.error("Polygon API key not found in environment variables.")
    raise Exception("Polygon API key not found in environment variables.")

# Trading Constants
TICKERS_URL = 'https://api.polygon.io/v3/reference/tickers'
RSI_URL_TEMPLATE = 'https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan=day&adjusted=true&window=14&series_type=close&order=desc&limit=1&apiKey={api_key}'
AGGREGATES_URL_TEMPLATE = 'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-01/2024-12-31?adjusted=true&sort=desc&limit=200&apiKey={api_key}'

MIN_RSI = 15
MAX_RSI = 40
VOLUME_INCREASE_THRESHOLD = 2.0  # 200% of average volume
CMF_PERIOD = 20  # Period for Chaikin Money Flow

# Timezone settings
EASTERN_TZ = pytz.timezone('US/Eastern')
MARKET_OPEN = datetime_time(4, 0, 0)
MARKET_CLOSE = datetime_time(20, 0, 0)

async def get_all_tickers(session):
    """
    Fetch all active stock tickers with proper pagination.
    """
    params = {
        'market': 'stocks',
        'active': 'true',
        'limit': 1000,
        'apiKey': POLYGON_API_KEY
    }
    
    all_tickers = []
    next_url = TICKERS_URL
    
    while next_url:
        try:
            async with session.get(next_url, params=params) as response:
                response_text = await response.text()
                logger.debug(f"Tickers API Response Status: {response.status}")
                
                if response.status != 200:
                    logger.error(f"Failed to fetch tickers: {response.status} - {response_text}")
                    break
                
                data = json.loads(response_text)
                if not data.get('results'):
                    break
                    
                new_tickers = [item['ticker'] for item in data['results']]
                all_tickers.extend(new_tickers)
                logger.debug(f"Added {len(new_tickers)} tickers")
                
                # Handle pagination
                next_url = data.get('next_url')
                if next_url:
                    params = {'apiKey': POLYGON_API_KEY}
                
                # Small delay to avoid rate limits
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in ticker pagination: {e}")
            break

    logger.info(f"Total tickers retrieved: {len(all_tickers)}")
    return all_tickers

def calculate_cmf(bars):
    """
    Calculate Chaikin Money Flow.
    """
    if not bars:
        return 0
        
    money_flow_vol_sum = 0
    volume_sum = 0
    
    for bar in bars:
        high = bar['h']
        low = bar['l']
        close = bar['c']
        volume = bar['v']
        
        money_flow_multiplier = ((close - low) - (high - close)) / (high - low) if high != low else 0
        money_flow_volume = money_flow_multiplier * volume
        
        money_flow_vol_sum += money_flow_volume
        volume_sum += volume
    
    return money_flow_vol_sum / volume_sum if volume_sum > 0 else 0

async def get_ticker_data(session, ticker):
    """
    Get comprehensive ticker data including RSI, volume, and CMF.
    """
    rsi_url = RSI_URL_TEMPLATE.format(ticker=ticker, api_key=POLYGON_API_KEY)
    volume_url = AGGREGATES_URL_TEMPLATE.format(ticker=ticker, api_key=POLYGON_API_KEY)
    
    try:
        async with asyncio.TaskGroup() as tg:
            rsi_task = tg.create_task(session.get(rsi_url))
            volume_task = tg.create_task(session.get(volume_url))
        
        rsi_response = rsi_task.result()
        volume_response = volume_task.result()
        
        if rsi_response.status != 200 or volume_response.status != 200:
            logger.debug(f"API error for {ticker}: RSI={rsi_response.status}, Volume={volume_response.status}")
            return ticker, None
            
        rsi_data = await rsi_response.json()
        volume_data = await volume_response.json()
        
        if not all([rsi_data.get('results'), volume_data.get('results')]):
            logger.debug(f"No data for {ticker}")
            return ticker, None
        
        rsi_value = rsi_data['results']['values'][0]['value']
        
        # Calculate volume metrics
        volumes = [bar['v'] for bar in volume_data['results']]
        current_volume = volumes[0]
        avg_volume = sum(volumes[1:21]) / 20  # 20-day average
        volume_increase = current_volume / avg_volume if avg_volume > 0 else 0
        
        # Calculate CMF
        cmf = calculate_cmf(volume_data['results'][:CMF_PERIOD])
        
        data = {
            'rsi': rsi_value,
            'volume_increase': volume_increase,
            'cmf': cmf,
            'current_volume': current_volume,
            'avg_volume': avg_volume
        }
        
        logger.debug(f"Processed {ticker}: RSI={rsi_value:.2f}, Volume Increase={volume_increase:.2f}x, CMF={cmf:.2f}")
        return ticker, data
        
    except Exception as e:
        logger.error(f"Error processing {ticker}: {e}")
        return ticker, None

async def process_tickers(tickers):
    """
    Process all tickers simultaneously with full analysis.
    """
    connector = aiohttp.TCPConnector(limit=100)  # Limit concurrent connections
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [get_ticker_data(session, ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks)
        
        filtered_results = {
            ticker: data for ticker, data in results
            if data and 
            MIN_RSI <= data['rsi'] <= MAX_RSI and 
            data['volume_increase'] >= VOLUME_INCREASE_THRESHOLD and 
            data['cmf'] > 0
        }
        
        logger.info(f"Found {len(filtered_results)} stocks matching criteria")
        return filtered_results

def save_results(results):
    """
    Save analysis results to JSON file.
    """
    with open('entry.json', 'w') as f:
        json.dump(results, f, indent=4)
    logger.info(f"Saved analysis results for {len(results)} stocks")

def is_market_open():
    """Check if the market is currently open."""
    now = datetime.now(EASTERN_TZ).time()
    return MARKET_OPEN <= now <= MARKET_CLOSE

async def main():
    """
    Main execution loop with full parallel processing.
    """
    logger.info("Starting market scanner")

    try:
        while True:
            if is_market_open():
                start_time = time.time()
                logger.info("Starting scan cycle")

                try:
                    async with aiohttp.ClientSession() as session:
                        tickers = await get_all_tickers(session)
                        if tickers:
                            results = await process_tickers(tickers)
                            save_results(results)

                    elapsed_time = time.time() - start_time
                    logger.info(f"Scan cycle completed in {elapsed_time:.2f} seconds")
                except Exception as e:
                    logger.error(f"Error in scan cycle: {e}")

                await asyncio.sleep(300)  # 5 minute wait
            else:
                logger.info("Market closed - waiting for next session")
                now = datetime.now(EASTERN_TZ)
                next_market_open = datetime.combine(now.date(), MARKET_OPEN, tzinfo=EASTERN_TZ)
                if now.time() > MARKET_CLOSE:
                    next_market_open += timedelta(days=1)
                sleep_seconds = (next_market_open - now).total_seconds()
                await asyncio.sleep(sleep_seconds)

    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
        raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scanner terminated by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")