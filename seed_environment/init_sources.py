"""
seed_environment/init_sources.py
=================================
Script one-shot : initialise les sources de données dans Docker.
  - Crée le bucket MinIO `ecommerce-raw` et y uploade des CSV de test
  - Crée les tables `customers` et `products` dans PostgreSQL et les remplit

Lancement :
    python seed_environment/init_sources.py
"""

import io
import os
import textwrap

import psycopg2
from minio import Minio

# ---- Connexions (identiques à ingestion_flow.py) ----
POSTGRES_CONN = dict(
    host="127.0.0.1", port=5433,
    user="dataops", password="dataops", dbname="oltp"
)
MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)
BUCKET = "ecommerce-raw"


# ---- Données de test ----
ORDERS_CSV = textwrap.dedent("""\
    order_id,customer_id,order_date,status,total_amount
    1,101,2024-01-15,delivered,250.00
    2,102,2024-01-16,shipped,89.99
    3,103,2024-01-17,pending,430.50
    4,101,2024-01-18,delivered,120.00
    5,104,2024-01-19,cancelled,75.00
""")

ORDER_ITEMS_CSV = textwrap.dedent("""\
    item_id,order_id,product_id,quantity,unit_price
    1,1,201,2,75.00
    2,1,202,1,100.00
    3,2,203,1,89.99
    4,3,201,3,75.00
    5,3,204,1,205.50
    6,4,202,1,120.00
    7,5,205,1,75.00
""")


def init_minio():
    print("=== MinIO ===")
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)
        print(f"  Bucket '{BUCKET}' créé.")
    else:
        print(f"  Bucket '{BUCKET}' existe déjà.")

    for name, content in [("orders.csv", ORDERS_CSV), ("order_items.csv", ORDER_ITEMS_CSV)]:
        data = content.encode("utf-8")
        MINIO_CLIENT.put_object(
            BUCKET, name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="text/csv",
        )
        print(f"  Uploadé : {name} ({len(data)} bytes)")


def init_postgres():
    print("=== PostgreSQL ===")
    con = psycopg2.connect(**POSTGRES_CONN)
    con.autocommit = True
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS customers CASCADE")
    cur.execute("""
        CREATE TABLE customers (
            customer_id   SERIAL PRIMARY KEY,
            first_name    VARCHAR(50),
            last_name     VARCHAR(50),
            email         VARCHAR(100) UNIQUE,
            country       VARCHAR(50),
            created_at    TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        INSERT INTO customers (customer_id, first_name, last_name, email, country)
        VALUES
            (101, 'Alice',   'Martin',  'alice@example.com',  'France'),
            (102, 'Bob',     'Dupont',  'bob@example.com',    'Belgique'),
            (103, 'Claire',  'Leroy',   'claire@example.com', 'France'),
            (104, 'David',   'Bernard', 'david@example.com',  'Suisse')
    """)
    print("  Table 'customers' prête (4 lignes).")

    cur.execute("DROP TABLE IF EXISTS products CASCADE")
    cur.execute("""
        CREATE TABLE products (
            product_id    SERIAL PRIMARY KEY,
            name          VARCHAR(100),
            category      VARCHAR(50),
            unit_price    NUMERIC(10,2),
            stock         INTEGER
        )
    """)
    cur.execute("""
        INSERT INTO products (product_id, name, category, unit_price, stock)
        VALUES
            (201, 'T-shirt coton',    'Vêtements',    75.00, 200),
            (202, 'Jean slim',        'Vêtements',   100.00, 150),
            (203, 'Casquette',        'Accessoires',  89.99,  80),
            (204, 'Veste cuir',       'Vêtements',   205.50,  40),
            (205, 'Ceinture',         'Accessoires',  75.00, 120)
    """)
    print("  Table 'products' prête (5 lignes).")

    cur.close()
    con.close()


if __name__ == "__main__":
    init_minio()
    init_postgres()
    print("\nInitialisation terminée. Lance maintenant : python flow/ingestion_flow.py")
