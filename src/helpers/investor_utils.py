import json
import threading
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer, KafkaProducer


class InvestorEngine:
    def __init__(self, portfolios, investor_name):
        self.portfolios = portfolios
        self.investor_name = investor_name
        self.all_watched_stocks = {s for p in portfolios.values() for s in p}
        self.daily_cache = {}
        self.prev_navs = {p_name: None for p_name in portfolios}
        self.lock = threading.Lock()

        self.producer = KafkaProducer(
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            metadata_max_age_ms=30000,
            acks='all'
        )

        # One SparkSession per investor process — identified by investor name
        self.spark = SparkSession.builder \
            .appName(f"Investor_{investor_name}") \
            .master("local[2]") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

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

            if not missing:
                print(f"--- [OK] Gate opened for {date}. Calculating NAV... ---")
                self.calculate_daily_metrics(date)

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

        def spark_listener():
            """
            Reads newline-delimited JSON from SE1 via Spark socketTextStream.
            Each line is a JSON string; foreachRDD dispatches to process_message.
            """
            ssc = self.spark.sparkContext

            # socketTextStream connects to SE1 as a Spark streaming client.
            # SE1 already serves newline-delimited JSON — no changes needed there.
            stream = self.spark \
                .readStream \
                .format("socket") \
                .option("host", "localhost") \
                .option("port", 9999) \
                .load()

            def handle_batch(batch_df, epoch_id):
                rows = batch_df.collect()
                for row in rows:
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
            """Consumes SE2 price messages from the StockExchange Kafka topic."""
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

        # Spark stream runs in a daemon thread; Kafka listener blocks the main thread
        threading.Thread(target=spark_listener, daemon=True, name=f'{self.investor_name}-spark').start()
        kafka_listener()
