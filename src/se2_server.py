import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer

from src.helpers.date import get_next_trading_day
from src.helpers import config


def on_send_success(record_metadata):
    print(f"[SE2] Sent → topic={record_metadata.topic} "
          f"partition={record_metadata.partition} "
          f"offset={record_metadata.offset}")

def on_send_error(excp):
    print(f"[SE2] Kafka send error: {excp}")


def run_server():
    producer = KafkaProducer(
        bootstrap_servers=['127.0.0.1:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=5000,
        metadata_max_age_ms=30000,
        acks='all'
    )

    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[12:]}
    current_date = datetime(2020, 1, 1)
    topic = 'StockExchange'

    print(f"SE2 Server ready: emitting to Kafka topic '{topic}'...")

    try:
        while True:
            valid_date = get_next_trading_day(current_date)
            date_str = valid_date.strftime('%Y-%m-%d')
            print(f"[SE2] Emitting date {date_str}")

            for ticker in current_prices:
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))
                msg = {
                    "date":   date_str,
                    "ticker": ticker,
                    "price":  round(current_prices[ticker], 2)
                }
                (producer
                    .send(topic, value=msg)
                    .add_callback(on_send_success)
                    .add_errback(on_send_error))

            producer.flush()
            current_date = valid_date
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[SE2] Server shutting down.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_server()
