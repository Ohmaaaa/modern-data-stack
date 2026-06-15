# Modern Data Stack — Pipeline E-commerce

Pipeline de données de bout en bout sur une stack moderne locale :
**PostgreSQL / MinIO → Prefect → DuckDB → dbt → Streamlit**

---

## Architecture

```
Sources (Docker)          Orchestration       Warehouse local    Visualisation
─────────────────         ─────────────       ───────────────    ─────────────
PostgreSQL :5433  ──┐                         DuckDB
  customers           ├──► Prefect Flow  ──►  staging.*   ──► dbt ──► main_analytics.*  ──► Streamlit
  products            │    (ingestion_flow)    orders                  mart_revenue_by_customer
                      │                        order_items             mart_revenue_by_category
MinIO :9000      ──┘                           customers               mart_orders_summary
  orders.csv                                   products
  order_items.csv
                                        Qualité des données
                                        ───────────────────
                                        Soda Core (sources + staging + analytics)
                                        dbt tests (36 tests — not_null, unique, accepted_values)
```

## Stack technique

| Outil | Rôle | Version |
|---|---|---|
| **DuckDB** | Data Warehouse local (fichier) | 1.1.3 |
| **dbt-duckdb** | Transformation SQL (ELT) | 1.8.4 |
| **Prefect** | Orchestration des flows | 3.1.5 |
| **Streamlit** | Dashboard KPIs | 1.40.1 |
| **Soda Core** | Data Quality checks | 3.3.20 |
| **PostgreSQL** | Source OLTP (Docker) | 16 |
| **MinIO** | Stockage fichiers bruts (Docker) | latest |

## Structure du projet

```
.
├── docker-compose.yml          # Sources : PostgreSQL + MinIO
├── requirements.txt            # Dépendances Python
├── streamlit_app.py            # Dashboard KPIs
│
├── flow/
│   ├── ingestion_flow.py       # Prefect : extraction PostgreSQL + MinIO → DuckDB staging
│   └── pipeline_flow.py        # Prefect : pipeline complet (Soda + EL + dbt + Soda)
│
├── dbt_projet/
│   ├── dbt_project.yml
│   ├── profiles.yml.example    # Copier en profiles.yml et adapter le chemin
│   └── models/
│       ├── staging/            # Nettoyage et typage des sources
│       │   ├── sources.yml
│       │   ├── schema.yml      # Tests dbt (not_null, unique, accepted_values)
│       │   ├── stg_orders.sql
│       │   ├── stg_order_items.sql
│       │   ├── stg_customers.sql
│       │   └── stg_products.sql
│       ├── intermediate/
│       │   └── int_orders_enriched.sql   # Jointure orders + items + customers + products
│       └── mart/
│           ├── schema.yml      # Tests dbt sur les KPIs
│           ├── mart_revenue_by_customer.sql
│           ├── mart_revenue_by_category.sql
│           └── mart_orders_summary.sql
│
├── soda/
│   ├── configuration.yml       # Connexions Soda (DuckDB + PostgreSQL)
│   ├── checks_sources.yml      # 13 checks sur PostgreSQL source
│   ├── checks_staging.yml      # 19 checks sur DuckDB staging
│   ├── checks_reconciliation.yml
│   └── checks_analytics.yml    # 14 checks sur DuckDB analytics
│
└── seed_environment/
    └── init_sources.py         # Script d'initialisation des données de test
```

## Installation

### Prérequis
- Python 3.12
- Docker Desktop

### 1. Cloner et créer l'environnement

```bash
git clone https://github.com/<votre-repo>/modern-data-stack.git
cd modern-data-stack

python -m venv .venv
# Windows
.\.venv\Scripts\Activate.ps1
# Linux / Mac
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Configurer dbt

```bash
cp dbt_projet/profiles.yml.example dbt_projet/profiles.yml
# Éditer dbt_projet/profiles.yml si besoin (chemin vers warehouse.duckdb)
```

### 3. Démarrer les sources Docker

```bash
docker compose up -d
```

### 4. Initialiser les données de test

```bash
python seed_environment/init_sources.py
```

## Lancement

### Terminal 1 — Serveur Prefect
```bash
prefect server start
```

### Terminal 2 — Pipeline complet (une commande)
```bash
python flow/pipeline_flow.py
```

Ce flow exécute dans l'ordre :
1. **Soda** — qualité des sources PostgreSQL (13 checks)
2. **Prefect EL** — extraction PostgreSQL + MinIO → DuckDB `staging`
3. **Soda** — qualité du staging (19 checks)
4. **Réconciliation** — counts source vs staging
5. **dbt run** — transformation → `main_analytics` (8 modèles)
6. **dbt test** — 36 tests de qualité
7. **Soda** — qualité de l'analytics (14 checks)

### Terminal 3 — Dashboard
```bash
streamlit run streamlit_app.py
# Ouvre http://localhost:8501
```

## Tests dbt (36 tests)

| Couche | Type de test | Tables testées |
|---|---|---|
| Sources | `not_null`, `unique` | orders, order_items, customers, products |
| Staging | `not_null`, `unique`, `accepted_values` | stg_orders, stg_customers, stg_products, stg_order_items |
| Mart | `not_null`, `unique` | mart_revenue_by_customer, mart_revenue_by_category, mart_orders_summary |

```bash
cd dbt_projet
dbt test --profiles-dir .
# Done. PASS=36 WARN=0 ERROR=0 SKIP=0 TOTAL=36
```

## Soda Data Quality (46 checks)

```bash
# Sources PostgreSQL
soda scan -d postgres_source -c soda/configuration.yml soda/checks_sources.yml

# Staging DuckDB
soda scan -d duckdb_local -c soda/configuration.yml soda/checks_staging.yml

# Analytics DuckDB
soda scan -d duckdb_local -c soda/configuration.yml soda/checks_analytics.yml
```
