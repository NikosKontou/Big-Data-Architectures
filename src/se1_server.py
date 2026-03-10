import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import socket
import random
import json
import threading
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
    ssocket.listen(10)
    print(f"SE1 Server ready on port {PORT}")

    clients = []
    clients_lock = threading.Lock()

    def accept_loop():
        """Continuously accepts new investor connections in a background thread."""
        while True:
            try:
                c, addr = ssocket.accept()
                c.setblocking(True)
                with clients_lock:
                    clients.append(c)
                print(f"[SE1] Client connected from {addr} (total: {len(clients)})")
            except OSError:
                break

    threading.Thread(target=accept_loop, daemon=True, name='SE1-accept').start()

    def broadcast(msg_bytes):
        """Send msg_bytes to every connected client; drop the ones that fail."""
        dead = []
        with clients_lock:
            for c in clients:
                try:
                    c.sendall(msg_bytes)
                except (ConnectionResetError, BrokenPipeError, OSError):
                    print("[SE1] A client disconnected — removing.")
                    dead.append(c)
            for c in dead:
                c.close()
                clients.remove(c)

    try:
        while True:
            valid_date = get_next_trading_day(current_date)
            date_str = valid_date.strftime('%Y-%m-%d')
            print(f"[SE1] Emitting date {date_str}")

            for ticker in current_prices:
                current_prices[ticker] *= (1 + random.uniform(-0.03, 0.03))
                msg = json.dumps({
                    "date":   date_str,
                    "ticker": ticker,
                    "price":  round(current_prices[ticker], 2)
                }) + '\n'
                broadcast(msg.encode())

            current_date = valid_date
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[SE1] Server shutting down.")
    finally:
        with clients_lock:
            for c in clients:
                c.close()
        ssocket.close()


if __name__ == "__main__":
    import time
    run_server()
