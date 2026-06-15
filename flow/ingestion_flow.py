
import os
# Pointe sur le serveur Prefect local (prefect server start dans un terminal séparé)
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

from prefect import flow, task
import pandas as pd
import duckdb
from sqlalchemy import create_engine
from minio import Minio
from io import BytesIO


# -------------------------
# CONFIG
# -------------------------

DUCKDB_PATH = "warehouse.duckdb"

POSTGRES_CONN = "postgresql://dataops:dataops@127.0.0.1:5433/oltp"

MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET = "ecommerce-raw"


# -------------------------
# MINIO EXTRACT
# -------------------------

@task(persist_result=False)
def extract_orders_from_minio() -> pd.DataFrame:
    obj = MINIO_CLIENT.get_object(BUCKET, "orders.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df


@task(persist_result=False)
def extract_order_items_from_minio() -> pd.DataFrame:
    obj = MINIO_CLIENT.get_object(BUCKET, "order_items.csv")
    df = pd.read_csv(BytesIO(obj.read()))
    return df


# -------------------------
# POSTGRES EXTRACT
# -------------------------

@task(persist_result=False)
def extract_customers() -> pd.DataFrame:
    engine = create_engine(POSTGRES_CONN)
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM customers", conn)
    return df


@task(persist_result=False)
def extract_products() -> pd.DataFrame:
    engine = create_engine(POSTGRES_CONN)
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM products", conn)
    return df


# -------------------------
# CLEAN
# -------------------------

@task(persist_result=False)
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.drop_duplicates()
    return df


# -------------------------
# DUCKDB LOAD
# -------------------------

@task(persist_result=False)
def load_to_duckdb(df: pd.DataFrame, table_name: str) -> int:
    con = duckdb.connect(DUCKDB_PATH)

    con.execute("CREATE SCHEMA IF NOT EXISTS staging")

    # Convertit en str Python natif (évite le StringDtype nullable de Pandas 2.x
    # qui n'est pas reconnu par duckdb.register dans toutes les versions)
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)

    con.register("tmp_df", df)
    con.execute(f"""
        CREATE OR REPLACE TABLE staging.{table_name} AS
        SELECT * FROM tmp_df
    """)
    con.unregister("tmp_df")

    count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}").fetchone()[0]
    print(f"Loaded {table_name}: {count} rows")
    con.close()

    return count  # retourner le count permet à validate_load de dépendre de cette tâche


@task(persist_result=False)
def validate_load(table_name: str, loaded_count: int) -> int:
    # `loaded_count` crée une dépendance explicite avec load_to_duckdb :
    # Prefect ne lancera validate_load qu'une fois load_to_duckdb terminé.
    con = duckdb.connect(DUCKDB_PATH)
    count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}").fetchone()[0]
    sample = con.execute(f"SELECT * FROM staging.{table_name} LIMIT 5").df()
    con.close()

    print(f"\n📦 {table_name}: {count} rows")
    print(sample)

    return count


# -------------------------
# FLOW
# -------------------------

@flow(log_prints=True, persist_result=False)
def ingestion_flow():

    # Extract
    orders_raw      = extract_orders_from_minio()
    order_items_raw = extract_order_items_from_minio()
    customers_raw   = extract_customers()
    products_raw    = extract_products()

    # Clean  (chaque appel reçoit le DataFrame réel retourné par la task précédente)
    orders      = clean_dataframe(orders_raw)
    order_items = clean_dataframe(order_items_raw)
    customers   = clean_dataframe(customers_raw)
    products    = clean_dataframe(products_raw)

    # Load + validate (validate reçoit le count → dépendance explicite garantie)
    n_orders = load_to_duckdb(orders, "orders")
    validate_load("orders", n_orders)

    load_to_duckdb(order_items, "order_items")
    load_to_duckdb(customers, "customers")
    load_to_duckdb(products, "products")

    print("All sources loaded into DuckDB staging")


# -------------------------
# ENTRYPOINT
# -------------------------

if __name__ == "__main__":
    ingestion_flow()
