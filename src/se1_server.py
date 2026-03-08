import socket
import random
import time
import json
from datetime import datetime
from src.helpers.date import get_next_trading_day
from src.helpers import config

PORT = 9999

def run_server():
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[:12]}
    current_date = datetime(2020, 1, 1)

    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssocket.bind(('', PORT))
    ssocket.listen(1)
    ssocket.setblocking(False)  # non-blocking accept
    print(f"SE1 Server ready on port {PORT}")

    client = None

    try:
        while True:
            # Try to accept a new connection without blocking
            try:
                c, addr = ssocket.accept()
                client = c
                client.setblocking(False)  # non-blocking send
                print(f"Client connected from {addr}")
            except BlockingIOError:
                pass  # no client waiting, that's fine

            # Always advance the date and compute prices
            valid_date = get_next_trading_day(current_date)
            date_str = valid_date.strftime('%Y-%m-%d')

            for ticker in current_prices:
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))
                msg = json.dumps({
                    "date": date_str,
                    "ticker": ticker,
                    "price": round(current_prices[ticker], 2)
                })

                # Only send if a client is connected
                if client:
                    try:
                        client.send((msg + '\n').encode())
                    except (ConnectionResetError, BrokenPipeError, BlockingIOError):
                        print("Client disconnected.")
                        client.close()
                        client = None

            current_date = valid_date
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nServer shutting down.")
    finally:
        if client:
            client.close()
        ssocket.close()

if __name__ == "__main__":
    run_server()