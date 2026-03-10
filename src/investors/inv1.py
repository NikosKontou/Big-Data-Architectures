import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.helpers.investor_utils import InvestorEngine

PORTFOLIOS = {
    "P11": {"IBM": 13000, "AAPL": 22000, "META": 19000, "AMZN": 25000, "GOOG": 19000, "AVGO": 24000},
    "P12": {"VZ": 29000, "INTC": 26000, "AMD": 21000, "MSFT": 12000, "PLTR": 27000, "ORCL": 12000}
}

if __name__ == "__main__":
    InvestorEngine(PORTFOLIOS, "Inv1").start()
