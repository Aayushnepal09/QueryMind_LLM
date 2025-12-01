# populate_db.py
# Migrate Code-1 normalized SQLite DB -> Postgres (Code-2 style script structure)

import sqlite3
import datetime
from pathlib import Path

import psycopg2
from psycopg2 import extras

from utils import get_db_url


POSTGRES_CREATE_SQL = """
-- Drop Code 2 tables first (as requested)
DROP TABLE IF EXISTS admission_lab_results CASCADE;
DROP TABLE IF EXISTS admission_primary_diagnoses CASCADE;
DROP TABLE IF EXISTS admissions CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
DROP TABLE IF EXISTS lab_tests CASCADE;
DROP TABLE IF EXISTS diagnosis_codes CASCADE;
DROP TABLE IF EXISTS lab_units CASCADE;
DROP TABLE IF EXISTS languages CASCADE;
DROP TABLE IF EXISTS marital_statuses CASCADE;
DROP TABLE IF EXISTS races CASCADE;
DROP TABLE IF EXISTS genders CASCADE;
DROP TABLE IF EXISTS stage_labs CASCADE;
DROP TABLE IF EXISTS stage_diagnoses CASCADE;
DROP TABLE IF EXISTS stage_admissions CASCADE;
DROP TABLE IF EXISTS stage_patients CASCADE;

-- Drop Code 1 tables if they exist
DROP TABLE IF EXISTS order_detail CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS product_category CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS country CASCADE;
DROP TABLE IF EXISTS region CASCADE;

-- Recreate Code 1 schema (IDs preserved from SQLite, so NOT SERIAL)
CREATE TABLE region (
    region_id INTEGER PRIMARY KEY,
    region    TEXT NOT NULL
);

CREATE TABLE country (
    country_id INTEGER PRIMARY KEY,
    country    TEXT NOT NULL,
    region_id  INTEGER NOT NULL REFERENCES region(region_id)
);

CREATE TABLE customer (
    customer_id INTEGER PRIMARY KEY,
    first_name  TEXT NOT NULL,
    last_name   TEXT NOT NULL,
    address     TEXT NOT NULL,
    city        TEXT NOT NULL,
    country_id  INTEGER NOT NULL REFERENCES country(country_id)
);

CREATE TABLE product_category (
    product_category_id INTEGER PRIMARY KEY,
    product_category    TEXT NOT NULL,
    product_category_description TEXT NOT NULL
);

CREATE TABLE product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_unit_price NUMERIC(12,2) NOT NULL,
    product_category_id INTEGER NOT NULL REFERENCES product_category(product_category_id)
);

CREATE TABLE order_detail (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customer(customer_id),
    product_id  INTEGER NOT NULL REFERENCES product(product_id),
    order_date  DATE NOT NULL,
    quantity_ordered INTEGER NOT NULL
);
"""


def parse_sqlite_date(s):
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if len(s) == 8 and s.isdigit():
        return datetime.datetime.strptime(s, "%Y%m%d").date()
    # expect YYYY-MM-DD
    return datetime.date.fromisoformat(s[:10])


def verify_sqlite_tables(sqlite_conn):
    cur = sqlite_conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {r[0] for r in cur.fetchall()}

    expected = {"Region", "Country", "Customer", "ProductCategory", "Product", "OrderDetail"}
    missing = sorted(list(expected - tables))
    if missing:
        raise ValueError(
            f"SQLite DB is missing expected tables: {missing}\n"
            f"Found: {sorted(tables)}"
        )


def copy_table(sqlite_conn, pg_conn, sqlite_table, pg_table, sqlite_cols, pg_cols, transform=None, batch_size=50_000):
    s_cur = sqlite_conn.cursor()
    p_cur = pg_conn.cursor()

    s_col_list = ", ".join(sqlite_cols)
    p_col_list = ", ".join(pg_cols)

    s_cur.execute(f"SELECT {s_col_list} FROM {sqlite_table};")
    insert_sql = f"INSERT INTO {pg_table} ({p_col_list}) VALUES %s"

    total = 0
    while True:
        rows = s_cur.fetchmany(batch_size)
        if not rows:
            break

        if transform:
            rows = [transform(r) for r in rows]

        extras.execute_values(p_cur, insert_sql, rows, page_size=10_000)
        pg_conn.commit()
        total += len(rows)
        print(f"Inserted {total:,} rows into {pg_table}")

    p_cur.close()
    s_cur.close()


if __name__ == "__main__":


    SQLITE_DB_PATH = "normalized.db"

    if not Path(SQLITE_DB_PATH).exists():
        raise FileNotFoundError(f"SQLite DB not found: {SQLITE_DB_PATH}")


    DATABASE_URL = get_db_url()

    # Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_conn.execute("PRAGMA foreign_keys = ON;")
    verify_sqlite_tables(sqlite_conn)

    # Create Postgres tables
    print("Creating tables...")
    pg_conn = psycopg2.connect(DATABASE_URL)
    cur = pg_conn.cursor()
    cur.execute(POSTGRES_CREATE_SQL)
    pg_conn.commit()
    cur.close()
    pg_conn.close()
    print("Tables created successfully\n")

    # Reconnect for migration
    pg_conn = psycopg2.connect(DATABASE_URL)

    print("Migrating Region...")
    copy_table(
        sqlite_conn, pg_conn,
        "Region", "region",
        ["RegionID", "Region"],
        ["region_id", "region"]
    )

    print("Migrating Country...")
    copy_table(
        sqlite_conn, pg_conn,
        "Country", "country",
        ["CountryID", "Country", "RegionID"],
        ["country_id", "country", "region_id"]
    )

    print("Migrating Customer...")
    copy_table(
        sqlite_conn, pg_conn,
        "Customer", "customer",
        ["CustomerID", "FirstName", "LastName", "Address", "City", "CountryID"],
        ["customer_id", "first_name", "last_name", "address", "city", "country_id"]
    )

    print("Migrating ProductCategory...")
    copy_table(
        sqlite_conn, pg_conn,
        "ProductCategory", "product_category",
        ["ProductCategoryID", "ProductCategory", "ProductCategoryDescription"],
        ["product_category_id", "product_category", "product_category_description"]
    )

    print("Migrating Product...")
    copy_table(
        sqlite_conn, pg_conn,
        "Product", "product",
        ["ProductID", "ProductName", "ProductUnitPrice", "ProductCategoryID"],
        ["product_id", "product_name", "product_unit_price", "product_category_id"],
        transform=lambda r: (r[0], r[1], round(float(r[2]), 2), r[3])
    )

    print("Migrating OrderDetail...")
    copy_table(
        sqlite_conn, pg_conn,
        "OrderDetail", "order_detail",
        ["OrderID", "CustomerID", "ProductID", "OrderDate", "QuantityOrdered"],
        ["order_id", "customer_id", "product_id", "order_date", "quantity_ordered"],
        transform=lambda r: (r[0], r[1], r[2], parse_sqlite_date(r[3]), int(r[4]))
    )

    pg_conn.close()
    sqlite_conn.close()

    print("\n✅ SQLite → Postgres migration complete!")
