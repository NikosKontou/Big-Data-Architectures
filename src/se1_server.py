import socket
import random
import time
import json
from datetime import datetime, timedelta
from src.helpers.date import get_next_trading_day
from src.helpers import config

PORT = 9999

def create_connection():
    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssocket.bind(('', PORT))
    ssocket.listen(1)
    print(f"SE1 Server ready: listening on port {PORT}...")
    client_socket, addr = ssocket.accept()
    print(f"Connection established with {addr}")
    return client_socket

def run_server():
    # Convert to dict for easier daily price updates
    current_prices = {ticker: price for ticker, price in config.ALL_STOCKS[:12]}

    ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ssocket.bind(('', PORT))
    ssocket.listen(1)
    print(f"SE1 Server ready on port {PORT}")

    c, addr = ssocket.accept()
    current_date = datetime(2020, 1, 1)
    try:
        while True:
            valid_date = get_next_trading_day(current_date)
            date_str = valid_date.strftime('%Y-%m-%d')
            time.sleep(3)  # [cite: 10]

            for ticker in current_prices:
                # +/- 3% logic [cite: 9]
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))

                msg = json.dumps({
                    "date": date_str,
                    "ticker": ticker,
                    "price": round(current_prices[ticker], 2)
                })
                c.send((msg + '\n').encode())

            current_date = valid_date

    except (ConnectionResetError, BrokenPipeError):
        print("Client disconnected. Restarting server...")
        c.close()
        run_server()
    except KeyboardInterrupt:
        print("\nServer shutting down.")
        c.close()

if __name__ == "__main__":
    run_server()