import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.helpers.investor_utils import InvestorEngine

PORTFOLIOS = {
    "P31": {"HPQ": 22000, "ZM": 18000, "DELL": 24000, "NVDA": 12000, "IBM": 19000, "INTC": 16000},
    "P32": {"VZ": 18000, "AVGO": 29000, "NVDA": 16000, "AAPL": 22000, "DELL": 25000, "ORCL": 20000}
}

if __name__ == "__main__":
    InvestorEngine(PORTFOLIOS, "Inv3").start()
