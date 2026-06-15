import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
 
# ----------------------------------
# CONFIG
# ----------------------------------
 
NUM_CUSTOMERS = 50
NUM_PRODUCTS = 30
NUM_ORDERS = 200
NUM_ORDER_ITEMS = 500
 
os.makedirs("seed_output", exist_ok=True)
 
# ----------------------------------
# CUSTOMERS
# ----------------------------------
 
customers = pd.DataFrame({
    "customer_id": range(1, NUM_CUSTOMERS + 1),
    "country": np.random.choice(
        ["FR", "DE", "US", "UK", "ES"],
        NUM_CUSTOMERS
    ),
    "signup_date": pd.date_range(
        start="2023-01-01",
        periods=NUM_CUSTOMERS
    ),
    "segment": np.random.choice(
        ["retail", "enterprise", "vip"],
        NUM_CUSTOMERS
    )
})
 
# ----------------------------------
# PRODUCTS
# ----------------------------------
 
products = pd.DataFrame({
    "product_id": range(1, NUM_PRODUCTS + 1),
    "name": [f"Product_{i}" for i in range(1, NUM_PRODUCTS + 1)],
    "category": np.random.choice(
        ["electronics", "clothing", "sports", "home"],
        NUM_PRODUCTS
    ),
    "cost": np.round(
        np.random.uniform(5, 200, NUM_PRODUCTS),
        2
    )
})
 
# ----------------------------------
# ORDERS
# ----------------------------------
 
orders = pd.DataFrame({
    "order_id": range(1, NUM_ORDERS + 1),
    "customer_id": np.random.randint(
        1,
        NUM_CUSTOMERS + 1,
        NUM_ORDERS
    ),
    "store_id": np.random.randint(
        1,
        10,
        NUM_ORDERS
    ),
    "order_date": pd.date_range(
        start="2024-01-01",
        periods=NUM_ORDERS
    ),
    "status": np.random.choice(
        ["pending", "shipped", "cancelled"],
        NUM_ORDERS
    )
})
 
# ----------------------------------
# ORDER ITEMS
# ----------------------------------
 
order_items = pd.DataFrame({
    "item_id": range(1, NUM_ORDER_ITEMS + 1),
    "order_id": np.random.randint(
        1,
        NUM_ORDERS + 1,
        NUM_ORDER_ITEMS
    ),
    "product_id": np.random.randint(
        1,
        NUM_PRODUCTS + 1,
        NUM_ORDER_ITEMS
    ),
    "quantity": np.random.randint(
        1,
        5,
        NUM_ORDER_ITEMS
    ),
    "unit_price": np.round(
        np.random.uniform(
            10,
            300,
            NUM_ORDER_ITEMS
        ),
        2
    )
})
 
# ----------------------------------
# CHAOS ANOMALY
# ----------------------------------
 
anomaly_index = np.random.randint(
    0,
    NUM_ORDER_ITEMS
)
 
order_items.loc[
    anomaly_index,
    "unit_price"
] = -99.99
 
print(
    f"Injected anomaly at row {anomaly_index}"
)
 
# ----------------------------------
# SAVE CSV FILES
# ----------------------------------
 
orders.to_csv(
    "seed_output/orders.csv",
    index=False
)
 
order_items.to_csv(
    "seed_output/order_items.csv",
    index=False
)
 
customers.to_csv(
    "seed_output/customers.csv",
    index=False
)
 
products.to_csv(
    "seed_output/products.csv",
    index=False
)
 
# ----------------------------------
# LOAD POSTGRES TABLES
# ----------------------------------
 
engine = create_engine(
    "postgresql://dataops:dataops@127.0.0.1:5433/oltp"
)
 
customers.to_sql(
    "customers",
    engine,
    if_exists="replace",
    index=False
)
 
products.to_sql(
    "products",
    engine,
    if_exists="replace",
    index=False
)
 
print("Seed completed successfully.")