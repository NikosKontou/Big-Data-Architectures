# CREATE USER 'bigdatauser'@'localhost' IDENTIFIED BY 'bigdatapass';
# GRANT ALL PRIVILEGES ON *.* TO 'bigdatauser'@'localhost' WITH GRANT OPTION;
# FLUSH PRIVILEGES;

import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='bigdatauser',
    password='bigdatapass'
)
cursor = conn.cursor()

# Create & select database
cursor.execute("CREATE DATABASE IF NOT EXISTS InvestorsDB DEFAULT CHARACTER SET utf8mb4")
cursor.execute("USE InvestorsDB")
print("[DB] Database 'InvestorsDB' ready.")

# Table definitions
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Investors (
        Id   INT          NOT NULL AUTO_INCREMENT,
        Name VARCHAR(100) NOT NULL,
        City VARCHAR(100) NOT NULL,
        PRIMARY KEY (Id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Portfolios (
        Id         INT          NOT NULL AUTO_INCREMENT,
        Name       VARCHAR(50)  NOT NULL UNIQUE,
        Cumulative DOUBLE       NOT NULL DEFAULT 0,
        PRIMARY KEY (Id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Investors_Portfolios (
        iid INT NOT NULL,
        pid INT NOT NULL,
        PRIMARY KEY (iid, pid),
        FOREIGN KEY (iid) REFERENCES Investors(Id),
        FOREIGN KEY (pid) REFERENCES Portfolios(Id)
    )
""")
print("[DB] Tables Investors, Portfolios, Investors_Portfolios ready.")

# Seed data
investors = [
    (1, 'Inv1', 'Athens'),
    (2, 'Inv2', 'Thessaloniki'),
    (3, 'Inv3', 'Patras'),
]

portfolios = [
    (1,  'P11'), (2,  'P12'),
    (3,  'P21'), (4,  'P22'),
    (5,  'P31'), (6,  'P32'),
]

investors_portfolios = [
    (1, 1), (1, 2),   # Inv1 → P11, P12
    (2, 3), (2, 4),   # Inv2 → P21, P22
    (3, 5), (3, 6),   # Inv3 → P31, P32
]

cursor.executemany(
    "INSERT IGNORE INTO Investors (Id, Name, City) VALUES (%s, %s, %s)",
    investors
)

cursor.executemany(
    "INSERT IGNORE INTO Portfolios (Id, Name) VALUES (%s, %s)",
    portfolios
)

cursor.executemany(
    "INSERT IGNORE INTO Investors_Portfolios (iid, pid) VALUES (%s, %s)",
    investors_portfolios
)

conn.commit()
print("[DB] Seed data inserted into Investors, Portfolios, Investors_Portfolios.")

# Per-investor/portfolio daily tables
# One table per (investor, portfolio) pair, e.g. Inv1_P11, Inv2_P22, etc.
portfolio_map = {
    'Inv1': ['P11', 'P12'],
    'Inv2': ['P21', 'P22'],
    'Inv3': ['P31', 'P32'],
}

for investor, portfolios_list in portfolio_map.items():
    for portfolio in portfolios_list:
        table_name = f"{investor}_{portfolio}"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                Date              DATE         NOT NULL,
                NAV_per_Share     DOUBLE       NOT NULL,
                Daily_NAV_Change  DOUBLE       NOT NULL DEFAULT 0,
                Daily_NAV_Change_Pct DOUBLE    NOT NULL DEFAULT 0,
                PRIMARY KEY (Date)
            )
        """)
        print(f"[DB] Table '{table_name}' ready.")

conn.commit()
print("[DB] All tables created successfully.")

cursor.close()
conn.close()
