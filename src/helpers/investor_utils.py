import json
import threading
import random
from pyspark.sql import SparkSession
from kafka import KafkaConsumer, KafkaProducer


class InvestorEngine:
    def __init__(self, portfolios, investor_name):
        self.portfolios = portfolios
        self.investor_name = investor_name

        # SE1 owns ALL_STOCKS[:12], SE2 owns ALL_STOCKS[12:]
        # Each investor watches a subset — split by which exchange owns them
        self.all_watched_stocks = {s for p in portfolios.values() for s in p}

        self.daily_cache = {}   # date -> {ticker -> price}
        self.prev_navs = {p_name: None for p_name in portfolios}
        self.lock = threading.Lock()

        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            metadata_max_age_ms=30000,
            acks='all'
        )

        self.spark = SparkSession.builder \
            .appName(f"Investor_{investor_name}") \
            .master("local[2]") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

        # Populated once start() is called, after se1/se2 stock sets are known
        self.se1_watched = set()  # watched stocks that come from SE1 (TCP)
        self.se2_watched = set()  # watched stocks that come from SE2 (Kafka)

    def _set_source_splits(self, se1_tickers, se2_tickers):
        """
        Called by start() to tell the engine which of its watched stocks
        belong to SE1 and which to SE2. Only watched stocks matter — the
        gate uses these sets to know when each source is complete for a date.
        """
        self.se1_watched = self.all_watched_stocks & se1_tickers
        self.se2_watched = self.all_watched_stocks & se2_tickers
        print(f"[{self.investor_name}] SE1 watched: {self.se1_watched}")
        print(f"[{self.investor_name}] SE2 watched: {self.se2_watched}")

    def process_message(self, data):
        with self.lock:
            date, ticker, price = data['date'], data['ticker'], data['price']

            if ticker not in self.all_watched_stocks:
                return

            self.daily_cache.setdefault(date, {})[ticker] = price

            received       = set(self.daily_cache[date].keys())
            missing        = self.all_watched_stocks - received
            current_count  = len(received)
            required_count = len(self.all_watched_stocks)

            print(f"[{self.investor_name}] Date: {date} | "
                  f"Collected: {current_count}/{required_count} | "
                  f"Just added: {ticker} | "
                  f"Still missing: {missing if missing else 'none'}")

            # Gate: only fire when ALL tickers from BOTH sources have arrived
            se1_complete = self.se1_watched.issubset(received)
            se2_complete = self.se2_watched.issubset(received)

            if se1_complete and se2_complete:
                print(f"--- [OK] Gate opened for {date} (SE1 ✓  SE2 ✓). Calculating NAV... ---")
                self.calculate_daily_metrics(date)
            else:
                waiting = []
                if not se1_complete:
                    waiting.append(f"SE1 missing {self.se1_watched - received}")
                if not se2_complete:
                    waiting.append(f"SE2 missing {self.se2_watched - received}")
                print(f"[{self.investor_name}] Waiting: {' | '.join(waiting)}")

    def calculate_daily_metrics(self, date):
        """Calculates NAV, Daily_Change, and Daily_Change_Percent for each portfolio."""
        prices = self.daily_cache[date]

        for p_name, p_qty in self.portfolios.items():
            total_assets = sum(prices[s] * qty for s, qty in p_qty.items())
            liabilities  = total_assets * random.uniform(0.65, 0.70)
            current_nav  = total_assets - liabilities

            change     = 0.0
            pct_change = 0.0
            if self.prev_navs[p_name] is not None:
                change     = current_nav - self.prev_navs[p_name]
                pct_change = (change / self.prev_navs[p_name]) * 100

            self.prev_navs[p_name] = current_nav

            payload = {
                "Date":                 date,
                "NAV":                  round(current_nav, 2),
                "Daily_Change":         round(change, 2),
                "Daily_Change_Percent": round(pct_change, 2),
            }

            self.producer.send('portfolios', key=p_name.encode('utf-8'), value=payload)
            print(f"[{self.investor_name}] Published {p_name} → key='{p_name}' | {payload}")

        self.producer.flush()
        del self.daily_cache[date]

    def start(self):
        """Runs the Spark (SE1/TCP) and Kafka (SE2) listeners concurrently."""
        from src.helpers import config

        # Derive which tickers belong to which exchange from config
        se1_tickers = {ticker for ticker, _ in config.ALL_STOCKS[:12]}
        se2_tickers = {ticker for ticker, _ in config.ALL_STOCKS[12:]}
        self._set_source_splits(se1_tickers, se2_tickers)

        def spark_listener():
            stream = self.spark \
                .readStream \
                .format("socket") \
                .option("host", "localhost") \
                .option("port", 9999) \
                .load()

            def handle_batch(batch_df, epoch_id):
                for row in batch_df.collect():
                    line = row[0].strip()
                    if line:
                        try:
                            self.process_message(json.loads(line))
                        except json.JSONDecodeError as e:
                            print(f"[{self.investor_name}] Bad JSON from SE1: {e}")

            query = stream.writeStream \
                .foreachBatch(handle_batch) \
                .trigger(processingTime='1 second') \
                .start()

            print(f"[{self.investor_name}] Spark stream connected to SE1 on port 9999.")
            query.awaitTermination()

        def kafka_listener():
            consumer = KafkaConsumer(
                'StockExchange',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id=None,
                auto_offset_reset='latest',
                enable_auto_commit=False,
            )
            print(f"[{self.investor_name}] Kafka listener subscribed to StockExchange (latest only).")
            for msg in consumer:
                self.process_message(msg.value)

        threading.Thread(target=spark_listener, daemon=True, name=f'{self.investor_name}-spark').start()
        kafka_listener()
