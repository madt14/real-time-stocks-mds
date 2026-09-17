import json
import os
import time
 
from curl_cffi import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
 
load_dotenv()
 
API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
TOPIC = "stock-quotes"
KAFKA_SERVER = "localhost:29092"
MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2
 
if not API_KEY:
    raise RuntimeError("FINNHUB_API_KEY is missing. Put it in your .env file.")
 
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)
 
 
def fetch_quote(symbol: str):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # impersonate="chrome" makes the TLS handshake indistinguishable
            # from a real Chrome browser's, which is what Finnhub's Cloudflare
            # edge is checking for.
            response = requests.get(url, timeout=10, impersonate="chrome")
            response.raise_for_status()
            data = response.json()
            data["symbol"] = symbol
            data["fetched_at"] = int(time.time())
            return data
        except Exception as exc:
            print(f"ERROR fetching {symbol} (attempt {attempt}/{MAX_ATTEMPTS}): {exc}")
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAY_SECONDS)
    print(f"Giving up on {symbol} for this cycle.")
    return None
 
 
def main():
    print("Starting Kafka producer...")
    print(f"Symbols: {SYMBOLS}")
    print(f"Kafka: {KAFKA_SERVER}")
    print()
    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)
            if quote:
                print(f"Producing {symbol}: {quote}")
                producer.send(TOPIC, value=quote)
        producer.flush()
        # Five symbols + delay keeps request volume modest.
        time.sleep(6)
 
 
if __name__ == "__main__":
    main()