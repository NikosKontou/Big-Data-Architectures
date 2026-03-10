import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.helpers.investor_utils import InvestorEngine

PORTFOLIOS = {
    "P21": {"HPQ": 16000, "CSCO": 17000, "ZM": 19000, "QCOM": 21000, "ADBE": 28000, "VZ": 17000},
    "P22": {"TXN": 14000, "CRM": 26000, "AVGO": 17000, "NVDA": 18000, "MSTR": 26000, "MSI": 18000}
}

if __name__ == "__main__":
    InvestorEngine(PORTFOLIOS, "Inv2").start()
