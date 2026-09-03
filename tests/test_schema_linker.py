import sqlite3

import pytest

from sqlsentinel.schema_linker import Schema, Table, _terms, introspect


@pytest.fixture
def db(tmp_path):
    """A small schema with a join path: orders -> customers, orders -> products."""
    p = tmp_path / "shop.sqlite"
    conn = sqlite3.connect(p)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, full_name TEXT, city TEXT);
        CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT, price REAL);
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(id),
            product_id INTEGER REFERENCES products(id),
            qty INTEGER
        );
        CREATE TABLE unrelated_logs (id INTEGER PRIMARY KEY, message TEXT);
        INSERT INTO customers VALUES (1,'Ada','Paris'),(2,'Bo','Lima');
        INSERT INTO products VALUES (1,'Widget',9.5);
        INSERT INTO orders VALUES (1,1,1,3);
        INSERT INTO unrelated_logs VALUES (1,'noise');
        """
    )
    conn.commit()
    conn.close()
    return p


def test_finds_all_tables_and_columns(db):
    s = introspect(db)
    assert {t.name for t in s.tables} == {"customers", "products", "orders", "unrelated_logs"}
    assert s.n_columns == 3 + 3 + 4 + 2


def test_primary_keys_detected(db):
    s = introspect(db)
    orders = next(t for t in s.tables if t.name == "orders")
    assert [c.name for c in orders.columns if c.is_pk] == ["id"]


def test_foreign_keys_and_join_paths(db):
    s = introspect(db)
    paths = set(s.join_paths())
    assert "orders.customer_id -> customers.id" in paths
    assert "orders.product_id -> products.id" in paths


def test_row_counts(db):
    s = introspect(db)
    assert next(t for t in s.tables if t.name == "customers").row_count == 2


def test_sample_values_present_and_capped(db):
    s = introspect(db, sample_values=2)
    city = next(
        c for t in s.tables if t.name == "customers" for c in t.columns if c.name == "city"
    )
    assert 0 < len(city.samples) <= 2


def test_samples_can_be_disabled(db):
    s = introspect(db, sample_values=0)
    assert all(not c.samples for t in s.tables for c in t.columns)


def test_prompt_is_ddl_shaped(db):
    p = introspect(db).to_prompt()
    assert "CREATE TABLE customers" in p
    assert "PRIMARY KEY" in p
    assert "FOREIGN KEY" in p
    assert "rows" in p


def test_prompt_quotes_awkward_identifiers():
    s = Schema("x", [Table(name="my table")])
    assert "`my table`" in s.to_prompt()


def test_opens_read_only(db):
    """Introspection must never be able to modify a benchmark database."""
    introspect(db)
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 2
    conn.close()


# ---------------------------------------------------------------- pruning


def test_prune_keeps_the_relevant_table(db):
    s = introspect(db)
    kept = {t.name for t in s.prune("Which city is each customer in?").tables}
    assert "customers" in kept


def test_prune_pulls_back_fk_neighbours(db):
    """A pruned bridge table produces confidently wrong SQL, not an error."""
    s = introspect(db)
    kept = {t.name for t in s.prune("How many orders did each customer place?").tables}
    assert {"orders", "customers"} <= kept


def test_prune_never_returns_empty(db):
    s = introspect(db)
    assert len(s.prune("zzzz qqqq").tables) >= 1


def test_prune_with_no_matching_terms_returns_full_schema(db):
    s = introspect(db)
    assert len(s.prune("").tables) == len(s.tables)


def test_terms_splits_identifier_conventions():
    assert {"customer", "name"} <= _terms("customer_name")
    assert {"full", "name"} <= _terms("fullName")


def test_terms_drops_short_noise():
    assert "id" not in _terms("id of a")
