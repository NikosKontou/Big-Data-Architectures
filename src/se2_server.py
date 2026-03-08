import random
import time
import json
from datetime import datetime, timedelta
import holidays
from kafka import KafkaProducer

from src.helpers.date import get_next_trading_day
from src.helpers import config

def on_send_success(record_metadata):
    print(f"Successfully sent to {record_metadata.topic} "
          f"partition {record_metadata.partition} "
          f"offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error sending message: {excp}")

def run_server():
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=5000,
        metadata_max_age_ms=30000,
        acks='all'  # wait for broker acknowledgement
    )
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[12:]}

    topic = 'StockExchange'
    # Start date is 1/1/2020
    current_date = datetime(2020, 1, 1)

    print(f"SE2 Server ready: emitting to Kafka topic '{topic}'...")

    try:
        while True:
            print(f"[DEBUG] Iterating, current_date={current_date}, tickers={len(current_prices)}")
            valid_date = get_next_trading_day(current_date)
            date_str = valid_date.strftime('%Y-%m-%d')
            for ticker in current_prices:
                msg = {"date": date_str, "ticker": ticker, "price": round(current_prices[ticker], 2)}
                print(f"[DEBUG] Sending: {msg}")
                producer.send('StockExchange', value=msg).add_callback(on_send_success).add_errback(on_send_error)
            producer.flush()
            current_date = valid_date
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nSE2 Server shutting down.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_server()