import csv
from pathlib import Path

BASE = Path("include/data/landing")

DATA = {
    "2026-11-01/orders_1.csv": [
        {"order_id": "1001", "customer_id": "C01", "order_date": "2026-11-01", "amount": "250.00", "status": "delivered"},
        {"order_id": "1002", "customer_id": "C02", "order_date": "2026-11-01", "amount": "120.50", "status": "Pending"},
        {"order_id": "1003", "customer_id": "", "order_date": "2026-11-01", "amount": "80.00", "status": "delivered"},
        {"order_id": "1004", "customer_id": "C03", "order_date": "2026-11-01", "amount": "-15.00", "status": "cancelled"},
    ],
    "2026-11-01/orders_2.csv": [
        {"order_id": "1001", "customer_id": "C01", "order_date": "2026-11-01", "amount": "250.00", "status": "DELIVERED"},
        {"order_id": "1005", "customer_id": "C03", "order_date": "2026-11-01", "amount": "400.00", "status": "delivered"},
        {"order_id": "", "customer_id": "C04", "order_date": "2026-11-01", "amount": "90.00", "status": "delivered"},
    ],
    "2026-11-02/orders_1.csv": [
        {"order_id": "1006", "customer_id": "C04", "order_date": "2026-11-02", "amount": "abc", "status": "pending"},
        {"order_id": "1007", "customer_id": "C02", "order_date": "2026-11-02", "amount": "310.00", "status": "delivered"},
        {"order_id": "1008", "customer_id": "C01", "order_date": "2026-11-02", "amount": "150.00", "status": "DELIVERED"},
    ],
    "2026-11-03/orders_1.csv": [
        {"order_id": "1009", "customer_id": "C01", "order_date": "2026-11-03", "amount": "0", "status": "delivered"},
        {"order_id": "1010", "customer_id": "C04", "order_date": "2026-11-03", "amount": "520.00", "status": "delivered"},
        {"order_id": "1011", "customer_id": "C05", "order_date": "2026-11-03", "amount": "75.25", "status": "cancelled"},
    ],
    "2026-11-03/orders_2.csv": [
        {"order_id": "1012", "customer_id": "C02", "order_date": "2026-11-03", "amount": "190.00", "status": "delivered"},
        {"order_id": "10.5", "customer_id": "C05", "order_date": "2026-11-03", "amount": "50.00", "status": "pending"},
    ],
}

FIELDNAMES = ["order_id", "customer_id", "order_date", "amount", "status"]

for rel_path, rows in DATA.items():
    file_path = BASE / rel_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"created {file_path}")