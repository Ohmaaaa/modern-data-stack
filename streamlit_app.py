"""
Tableau de bord KPIs e-commerce.
Lit les tables du schéma `main_analytics` créées par dbt dans warehouse.duckdb.

Lancement :
    streamlit run streamlit_app.py
"""

from pathlib import Path
import duckdb
import streamlit as st

DUCKDB_PATH = str(Path(__file__).parent / "warehouse.duckdb")

st.set_page_config(page_title="E-commerce KPIs", page_icon="🛒", layout="wide")
st.title("🛒 E-commerce — Dashboard KPIs")
st.caption(f"Source : `{DUCKDB_PATH}` — schéma `main_analytics` (dbt)")

# ------------------------------------------------------------------
# Connexion
# ------------------------------------------------------------------
def q(sql: str):
    # Connexion fermée après chaque requête → DuckDB peut être ouvert par dbt en parallèle
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()

# Vérification que dbt a bien tourné
try:
    schemas = q("SELECT schema_name FROM information_schema.schemata").schema_name.tolist()
except Exception as e:
    st.error(f"Impossible d'ouvrir le warehouse : {e}")
    st.stop()

if "main_analytics" not in schemas:
    st.warning("Le schéma `main_analytics` est absent. Lance d'abord :\n\n"
               "1. `python flow/ingestion_flow.py`\n"
               "2. `dbt run --profiles-dir .` (dans dbt_projet/)")
    st.stop()

# ------------------------------------------------------------------
# Données
# ------------------------------------------------------------------
df_customers  = q("SELECT * FROM main_analytics.mart_revenue_by_customer")
df_categories = q("SELECT * FROM main_analytics.mart_revenue_by_category")
df_orders     = q("SELECT * FROM main_analytics.mart_orders_summary")

# ------------------------------------------------------------------
# Métriques globales
# ------------------------------------------------------------------
total_revenue   = df_customers["total_revenue"].sum()
total_orders    = df_orders["nb_orders"].sum()
total_customers = len(df_customers)

col1, col2, col3 = st.columns(3)
col1.metric("Chiffre d'affaires total", f"{total_revenue:,.2f} €")
col2.metric("Commandes totales", int(total_orders))
col3.metric("Clients actifs", total_customers)

st.divider()

# ------------------------------------------------------------------
# Répartition des commandes par statut
# ------------------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Commandes par statut")
    st.dataframe(
        df_orders[["status", "nb_orders", "revenue"]].rename(columns={
            "status": "Statut", "nb_orders": "Commandes", "revenue": "CA (€)"
        }),
        use_container_width=True,
        hide_index=True,
    )
    st.bar_chart(df_orders.set_index("status")["nb_orders"])

# ------------------------------------------------------------------
# CA par catégorie de produit
# ------------------------------------------------------------------
with col_right:
    st.subheader("CA par catégorie")
    st.dataframe(
        df_categories[["category", "units_sold", "total_revenue"]].rename(columns={
            "category": "Catégorie", "units_sold": "Unités vendues", "total_revenue": "CA (€)"
        }),
        use_container_width=True,
        hide_index=True,
    )
    st.bar_chart(df_categories.set_index("category")["total_revenue"])

st.divider()

# ------------------------------------------------------------------
# Top clients
# ------------------------------------------------------------------
st.subheader("Top clients par chiffre d'affaires")
st.dataframe(
    df_customers[["customer_name", "country", "total_orders", "total_revenue"]].rename(columns={
        "customer_name": "Client", "country": "Pays",
        "total_orders": "Commandes", "total_revenue": "CA (€)"
    }),
    use_container_width=True,
    hide_index=True,
)
st.bar_chart(df_customers.set_index("customer_name")["total_revenue"])

st.divider()
st.caption("Pipeline : PostgreSQL / MinIO → Prefect (EL) → DuckDB staging → dbt (stg + int + analytics) → Streamlit")
