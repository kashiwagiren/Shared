import re
import sqlite3
from datetime import datetime
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
TXT_FILE = os.path.join(script_dir, "students_unstructured_data.txt")
DB_FILE = os.path.join(script_dir, "student_etl.db")


def parse_date(text):
    patterns = [
        r"\d{4}-\d{2}-\d{2}",
        r"\d{4}/\d{2}/\d{2}",
        r"[A-Za-z]+\s+\d{2},\s+\d{4}"
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            raw_date = match.group(0)

            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%B %d, %Y"):
                try:
                    return datetime.strptime(raw_date, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    pass

    return None


def parse_name(text):
    patterns = [
        r"Name:\s*([A-Za-z ]+)",
        r"Student Name[=:]\s*([A-Za-z ]+)",
        r"Student[=:]\s*([A-Za-z ]+)",
        r"\d{4}-\d{2}-\d{2}\s*--\s*([A-Za-z ]+)\s*--",
        r"\]\s*([A-Za-z ]+)\s*-\s*[A-Z]",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()

    return "Unknown"


def parse_course(text):
    patterns = [
        r"Course[=:]\s*([A-Za-z0-9]+)",
        r"Program:\s*([A-Za-z0-9]+)",
        r"\s-\s(BS[A-Za-z0-9]+)\s-",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()

    return "Unknown"


def parse_score(text):
    patterns = [
        r"Score[=:]\s*(\d+)",
        r"score:\s*(\d+)",
        r"Grade:\s*(\d+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))

    return None


def parse_status(text, score):
    patterns = [
        r"Status[=:]\s*([A-Za-z]+)",
        r"Result:\s*([A-Za-z]+)",
        r"\b(passed|failed)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).capitalize()

    if score is not None:
        return "Passed" if score >= 75 else "Failed"

    return "Unknown"


def extract_and_transform():
    records = []

    with open(TXT_FILE, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if line == "":
                continue

            score = parse_score(line)

            record = {
                "date": parse_date(line),
                "name": parse_name(line),
                "course": parse_course(line),
                "score": score,
                "status": parse_status(line, score)
            }

            records.append(record)

    return records


def connect_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    return conn, cursor

# Answeranan
def create_table(cursor):
    """
    TODO #1:
    Write the SQLite3 CREATE TABLE statement.

    Table name: student_records

    Columns:
    id INTEGER PRIMARY KEY AUTOINCREMENT
    date TEXT
    name TEXT
    course TEXT
    score INTEGER
    status TEXT
    """

    # Write CREATE TABLE code here

    # the answer
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS student_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            name TEXT,
            course TEXT,
            score INTEGER,
            status TEXT
        )
    """)
    # ends here
    pass


def insert_records(cursor, records):
    """
    TODO #2:
    Write the SQLite3 INSERT INTO statement.

    Insert the following fields:
    date, name, course, score, status
    """

    # Write INSERT INTO code here

    # the answer
    for record in records:
        cursor.execute("""
            INSERT INTO student_records
            (date, name, course, score, status)
            VALUES (?, ?, ?, ?, ?)
        """, (
            record["date"],
            record["name"],
            record["course"],
            record["score"],
            record["status"]
        ))
    # ends here
    pass


def select_records(cursor):
    """
    TODO #3:
    Write the SQLite3 SELECT statement.

    Select all records from student_records.
    Display the result in the terminal.
    """

    # Write SELECT code here

    # the answer
    cursor.execute("SELECT * FROM student_records")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    # ends here
# Ends here

def main():
    records = extract_and_transform()

    conn, cursor = connect_database()

    create_table(cursor)
    insert_records(cursor, records)
    conn.commit()

    select_records(cursor)

    conn.close()


if __name__ == "__main__":
    main()