import socket
import json
import threading
import random
from kafka import KafkaConsumer, KafkaProducer


class InvestorEngine:
    def __init__(self, portfolios, investor_name):
        self.portfolios = portfolios
        self.investor_name = investor_name
        self.all_watched_stocks = {s for p in portfolios.values() for s in p}
        self.daily_cache = {}
        self.prev_navs = {p_name: None for p_name in portfolios}
        self.lock = threading.Lock()

        # Kafka Producer for 'portfolios' topic
        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            metadata_max_age_ms=30000,
            acks='all'
        )

    def process_message(self, data):
        with self.lock:
            date, ticker, price = data['date'], data['ticker'], data['price']

            if ticker not in self.all_watched_stocks:
                return

            self.daily_cache.setdefault(date, {})[ticker] = price

            received   = set(self.daily_cache[date].keys())
            missing    = self.all_watched_stocks - received
            current_count  = len(received)
            required_count = len(self.all_watched_stocks)

            print(f"[{self.investor_name}] Date: {date} | "
                  f"Collected: {current_count}/{required_count} | "
                  f"Just added: {ticker} | "
                  f"Still missing: {missing if missing else 'none'}")

            # Gate: open only when every required ticker has arrived,
            # regardless of the order messages come in across TCP and Kafka.
            if not missing:
                print(f"--- [OK] Gate opened for {date}. Calculating NAV... ---")
                self.calculate_daily_metrics(date)

    def calculate_daily_metrics(self, date):
        """Calculates NAV, Change, and % Change for each managed portfolio."""
        prices = self.daily_cache[date]

        for p_name, p_qty in self.portfolios.items():
            total_assets = sum(prices[s] * qty for s, qty in p_qty.items())

            # Simulated liabilities (65% to 70%)
            liabilities = total_assets * random.uniform(0.65, 0.70)

            current_nav = total_assets - liabilities

            change = 0.0
            pct_change = 0.0

            if self.prev_navs[p_name] is not None:
                change = current_nav - self.prev_navs[p_name]
                pct_change = (change / self.prev_navs[p_name]) * 100

            self.prev_navs[p_name] = current_nav

            payload = {
                "Date": date,
                "NAV": round(current_nav, 2),
                "Daily_Change": round(change, 2),
                "Daily_Change_Percent": round(pct_change, 2)
            }

            self.producer.send('portfolios', key=p_name.encode('utf-8'), value=payload)
            print(f"[{self.investor_name}] Published {p_name} stats for {date}")

        del self.daily_cache[date]

    def start(self):
        """Runs the TCP and Kafka listeners in separate threads."""

        def tcp_listener():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', 9999))
                f = s.makefile()
                for line in f:
                    if line:
                        self.process_message(json.loads(line))

        def kafka_listener():
            consumer = KafkaConsumer(
                'StockExchange',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # FIX 1: Give the consumer a stable group ID so offsets are
                # tracked and committed across restarts.
                group_id=f'investor-{self.investor_name}',
                # FIX 2: Read from the beginning so messages published before
                # this consumer started are not silently skipped.
                auto_offset_reset='earliest',
                # FIX 3: Removed api_version=(0,10) — let the client
                # auto-negotiate with the broker. Hardcoding an old version
                # causes silent failures on modern Kafka brokers (2.x+).
            )
            for msg in consumer:
                self.process_message(msg.value)

        threading.Thread(target=tcp_listener, daemon=True).start()
        kafka_listener()
