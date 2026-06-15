
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import psycopg2

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

from prefect import flow, task, get_run_logger
from ingestion_flow import (
    extract_orders_from_minio,
    extract_order_items_from_minio,
    extract_customers,
    extract_products,
    clean_dataframe,
    load_to_duckdb,
    validate_load,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR      = PROJECT_ROOT / "dbt_projet"
SODA_DIR     = PROJECT_ROOT / "soda"
DUCKDB_PATH  = str(PROJECT_ROOT / "warehouse.duckdb")
PYTHON       = sys.executable
POSTGRES_CONN = dict(host="127.0.0.1", port=5433, user="dataops", password="dataops", dbname="oltp")


# ================================================================
# TASK dbt
# ================================================================

@task(persist_result=False)
def run_dbt(command: str) -> None:
    logger = get_run_logger()
    dbt_bin = str(Path(PYTHON).parent / "dbt")
    result = subprocess.run(
        [dbt_bin, command, "--profiles-dir", "."],
        cwd=str(DBT_DIR), capture_output=True, text=True,
    )
    for line in result.stdout.splitlines():
        logger.info(line)
    for line in result.stderr.splitlines():
        logger.warning(line)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command} a échoué (code {result.returncode})")
    logger.info("dbt %s OK.", command)


# ================================================================
# TASK soda
# ================================================================

@task(persist_result=False)
def run_soda(checks_file: str, datasource: str) -> None:
    """Lance un scan Soda sur le fichier de checks indiqué."""
    logger = get_run_logger()
    soda_bin = str(Path(PYTHON).parent / "soda")
    checks_path = str(SODA_DIR / checks_file)
    config_path = str(SODA_DIR / "configuration.yml")

    result = subprocess.run(
        [soda_bin, "scan", "-d", datasource, "-c", config_path, checks_path],
        cwd=str(PROJECT_ROOT), capture_output=True, text=True,
    )
    for line in result.stdout.splitlines():
        logger.info(line)
    for line in result.stderr.splitlines():
        logger.warning(line)

    # Soda exit code : 0=OK, 1=warning, 2=failure, 3=error
    if result.returncode >= 2:
        raise RuntimeError(f"Soda scan {checks_file} a échoué (code {result.returncode})")
    logger.info("Soda %s OK.", checks_file)


# ================================================================
# TASK réconciliation source <-> staging (Python natif)
# ================================================================

@task(persist_result=False)
def reconcile_source_vs_staging() -> None:
    """
    Compare les counts entre PostgreSQL (source) et DuckDB staging.
    Lève une erreur si une table a perdu des lignes pendant l'ingestion.
    """
    logger = get_run_logger()

    # Counts côté source PostgreSQL
    pg = psycopg2.connect(**POSTGRES_CONN)
    cur = pg.cursor()
    source_counts = {}
    for table in ("customers", "products"):
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        source_counts[table] = cur.fetchone()[0]
    cur.close()
    pg.close()

    # Counts côté DuckDB staging
    duck = duckdb.connect(DUCKDB_PATH, read_only=True)
    staging_counts = {}
    for table in ("customers", "products"):
        staging_counts[table] = duck.execute(
            f"SELECT COUNT(*) FROM staging.{table}"
        ).fetchone()[0]
    duck.close()

    # Comparaison
    errors = []
    for table in ("customers", "products"):
        src = source_counts[table]
        stg = staging_counts[table]
        status = "OK" if src == stg else "MISMATCH"
        logger.info("Réconciliation %-12s — source=%d | staging=%d | %s", table, src, stg, status)
        if src != stg:
            errors.append(f"{table}: source={src} != staging={stg}")

    if errors:
        raise ValueError("Perte de données détectée pendant l'ingestion :\n" + "\n".join(errors))

    logger.info("Réconciliation source <-> staging : tout est cohérent.")


# ================================================================
# FLOW PRINCIPAL
# ================================================================

@flow(name="pipeline-complet", log_prints=True, persist_result=False)
def pipeline_flow() -> None:
    """
    Pipeline de bout en bout :
      1. Soda  — qualité des sources PostgreSQL
      2. EL    — ingestion PostgreSQL + MinIO → DuckDB staging
      3. Soda  — qualité du staging DuckDB
      4. Recon — réconciliation counts source vs staging
      5. dbt   — transformation staging → analytics
      6. dbt   — tests des modèles
      7. Soda  — qualité de la couche analytics
    """

    # ── 1. Qualité des sources ──────────────────────────────────
    run_soda("checks_sources.yml", "postgres_source")

    # ── 2. Ingestion (EL) ───────────────────────────────────────
    orders_raw      = extract_orders_from_minio()
    order_items_raw = extract_order_items_from_minio()
    customers_raw   = extract_customers()
    products_raw    = extract_products()

    orders      = clean_dataframe(orders_raw)
    order_items = clean_dataframe(order_items_raw)
    customers   = clean_dataframe(customers_raw)
    products    = clean_dataframe(products_raw)

    n_orders = load_to_duckdb(orders, "orders")
    validate_load("orders", n_orders)
    load_to_duckdb(order_items, "order_items")
    load_to_duckdb(customers, "customers")
    load_to_duckdb(products, "products")

    # ── 3. Qualité du staging ───────────────────────────────────
    run_soda("checks_staging.yml", "duckdb_local")

    # ── 4. Réconciliation source <-> staging ────────────────────
    reconcile_source_vs_staging()

    # ── 5 & 6. Transformation + tests dbt ──────────────────────
    run_dbt("run")
    run_dbt("test")

    # ── 7. Qualité de l'analytics ───────────────────────────────
    run_soda("checks_analytics.yml", "duckdb_local")

    print("Pipeline complet — main_analytics prêt pour Streamlit.")


if __name__ == "__main__":
    pipeline_flow()
