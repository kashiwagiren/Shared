"""
Structured Data ETL Practical Exam Answer Key
Topic: Load structured CSV data into SQLite3 using Python

This version contains the complete answer for:

1. CREATE TABLE
2. INSERT INTO
3. SELECT FROM
"""

import csv
import sqlite3
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(script_dir, "sales_data.csv")
DB_FILE = os.path.join(script_dir, "sales_etl.db")


def extract_data():
    records = []

    with open(CSV_FILE, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            records.append(row)

    return records


def transform_data(records):
    transformed_records = []

    for record in records:
        quantity = int(record["quantity"])
        unit_price = float(record["unit_price"])
        total_sales = quantity * unit_price

        transformed_record = {
            "date": record["date"],
            "product": record["product"],
            "category": record["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_sales": total_sales,
            "payment_method": record["payment_method"]
        }

        transformed_records.append(transformed_record)

    return transformed_records


def connect_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    return conn, cursor


def create_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            product TEXT,
            category TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_sales REAL,
            payment_method TEXT
        )
    """)


def insert_records(cursor, records):
    cursor.execute("DELETE FROM sales_records")

    for record in records:
        cursor.execute("""
            INSERT INTO sales_records (
                date,
                product,
                category,
                quantity,
                unit_price,
                total_sales,
                payment_method
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            record["date"],
            record["product"],
            record["category"],
            record["quantity"],
            record["unit_price"],
            record["total_sales"],
            record["payment_method"]
        ))


def select_records(cursor):
    cursor.execute("""
        SELECT id, date, product, category, quantity, unit_price, total_sales, payment_method
        FROM sales_records
    """)

    rows = cursor.fetchall()

    print("\nSALES RECORDS")
    print("-" * 110)
    print(
        f"{'ID':<5}"
        f"{'Date':<15}"
        f"{'Product':<18}"
        f"{'Category':<15}"
        f"{'Qty':<8}"
        f"{'Price':<12}"
        f"{'Total':<12}"
        f"{'Payment':<15}"
    )
    print("-" * 110)

    for row in rows:
        print(
            f"{row[0]:<5}"
            f"{row[1]:<15}"
            f"{row[2]:<18}"
            f"{row[3]:<15}"
            f"{row[4]:<8}"
            f"{row[5]:<12.2f}"
            f"{row[6]:<12.2f}"
            f"{row[7]:<15}"
        )


def main():
    raw_records = extract_data()
    clean_records = transform_data(raw_records)

    conn, cursor = connect_database()

    create_table(cursor)
    insert_records(cursor, clean_records)
    conn.commit()

    select_records(cursor)

    conn.close()


if __name__ == "__main__":
    main() 