# E-Commerce Lakehouse Analytics Pipeline | dbt + Databricks

An end-to-end e-commerce analytics engineering project built with **dbt Core** and **Databricks**. The pipeline transforms raw customer, order, order-item, and product data into tested, analytics-ready Gold-layer models for revenue, product, and customer reporting.

---

## Problem Statement

ShopFlow is an e-commerce business similar to Amazon. Its operational data is distributed across separate customer, order, product, and order-item datasets, making it difficult for business users to answer questions consistently.

This project builds a trusted analytics layer to support:

- Revenue and order-volume reporting
- Average order value and units sold
- Product and category performance analysis
- Customer and country-level analysis
- Historical customer attribute tracking

---

## Domain and Entities

The project models four core e-commerce entities.

| Entity | Description |
|---|---|
| Customers | Individuals who place orders |
| Orders | Transactions placed by customers |
| Order items | Individual products included within an order |
| Products | Product catalog records, including category and price |

### Business relationships

```text
Customers → Orders → Order Items → Products
```

- A customer can place many orders.
- An order can contain multiple order items.
- Each order item references one product.

---

## Data Model and Grain

The Gold layer uses two fact tables because the business needs reporting at two different levels of detail.

| Model | Grain | Purpose |
|---|---|---|
| `fct_orders` | One row per order | Revenue, order volume, average order value, customer analysis, and country reporting |
| `fct_order_items` | One row per order line item | Product and category performance, units sold, and item-level revenue |
| `dim_products` | One row per product | Product name, category, current price, and reusable product attributes |
| `scd_customers` | One row per customer version | Historical customer attributes for point-in-time analysis |

### Schema

The Schema includes two analytics fact tables, a product dimension, and a Type 2 Slowly Changing Dimension for customer history.

![image](https://github.com/SatyamSingh1299/ecommerce-lakehouse-dbt-databricks/blob/main/docs/star_schema.png)

---

## Architecture

The project follows a Medallion Architecture in Databricks.

```text
Raw CSV Seed Files
        │
        ▼
Bronze Layer
Raw customers, orders, products, and order items
        │
        ▼
Silver Layer
Standardized staging models and enriched intermediate models
        │
        ▼
Gold Layer
Analytics-ready facts, dimensions, customer history, and reporting models
```

| Layer | Schema | Purpose |
|---|---|---|
| Bronze | `bronze` | Loads raw customer, order, order-item, and product seed data |
| Silver | `silver` | Standardizes source data and creates reusable enriched datasets |
| Gold | `gold` | Publishes trusted facts, dimensions, customer history, and reporting models |

---

## dbt and Databricks Fundamentals

### dbt

| Feature | Implementation |
|---|---|
| Seeds | Loads raw e-commerce CSV files into the Bronze schema |
| Models | Builds SQL transformations across staging, intermediate, and mart layers |
| Incremental models | Uses the `merge` strategy for `fct_orders` and `fct_order_items` |
| Snapshots | Tracks customer attribute history using the `scd_customers` Type 2 SCD snapshot |
| Tests | Validates unique keys, required fields, accepted values, and relationships |
| Custom tests | Validates revenue, dates, product prices, quantities, and email values |
| Exposures | Documents downstream consumers such as executive and product analytics dashboards |
| Hooks | Captures pipeline activity at the start and end of dbt runs |
| Surrogate keys | Uses `dbt_utils.generate_surrogate_key` for `order_key` and `product_key` |

### Databricks

| Feature | Implementation |
|---|---|
| SQL Warehouse | Executes dbt-generated SQL transformations |
| Medallion schemas | Separates raw, standardized, and business-ready data into Bronze, Silver, and Gold |
| Databricks Asset Bundles | Defines Databricks jobs and deployment resources as code |
| GitHub Actions | Supports automated deployment to development and production environments |

---

## Transformation Details

### Bronze layer

Raw CSV files are loaded through dbt seeds into the `bronze` schema.

```text
raw_customers
raw_orders
raw_order_items
raw_products
```

The Bronze layer preserves source-level data and provides the foundation for downstream transformations.

### Silver layer

The Silver layer standardizes source data and builds reusable enriched datasets.

#### Staging models

Staging models maintain a one-to-one relationship with raw source data while standardizing formats and applying foundational data-quality tests.

| Model | Grain | Key transformations |
|---|---|---|
| `stg_products` | One row per product | Trims product and category text; casts `price` to `decimal(12,2)` |
| `stg_orders` | One row per order | Casts `order_date` to `date`; standardizes `status` using `lower(trim())`; casts `total_amount` to `decimal(12,2)` |
| `stg_order_items` | One row per order line item | Casts `quantity` to `int`; casts `unit_price` to `decimal(12,2)` |

Data-quality tests validate:

- Unique and non-null order, product, and order-item identifiers
- Required customer, order, product, and quantity fields
- Valid order statuses: `delivered`, `shipped`, `processing`, `returned`, and `cancelled`
- Referential integrity between orders, order items, products, and customers

#### Intermediate models

Intermediate models isolate joins, aggregations, and derived calculations before the data reaches the final marts.

| Model | Grain | Key transformations |
|---|---|---|
| `int_orders_enriched` | One row per order | Joins orders to the current customer record and aggregates line items into line count, total quantity, and calculated line-item revenue |
| `int_order_items_with_product` | One row per order line item | Joins order items with product and order context; calculates line revenue as `quantity × unit_price` |

For current-state reporting, `int_orders_enriched` joins orders to the active customer snapshot record where `dbt_valid_to is null`.

### Gold layer

The Gold layer publishes business-ready data models for dashboards, reporting, and ad hoc analysis.

| Model | Materialization | Key transformations |
|---|---|---|
| `fct_orders` | Incremental | Publishes order-level revenue, status, line count, total units, customer country, and `order_key` |
| `fct_order_items` | Incremental | Publishes item-level quantity, unit price, calculated revenue, product context, customer context, and order status |
| `dim_products` | Table or view | Publishes product attributes and `product_key` |
| `scd_customers` | Snapshot | Preserves historical customer attributes using Type 2 SCD logic |

The fact models use dbt's `merge` incremental strategy with `order_id` and `order_item_id` as unique keys. This allows the pipeline to process new or changed records without rebuilding all historical data on every run.

---

## Setup

### Prerequisites

- Python 3.10–3.12
- A Databricks workspace with a SQL Warehouse or compatible compute
- Databricks host, SQL Warehouse HTTP path, and access token or OAuth credentials

### Install

```bash
git clone <your-repo-url>
cd ecommerce-lakehouse-dbt-databricks

python3.12 -m pip install -r requirements.txt
cp profiles.yml.example profiles.yml
```

### Configure Databricks connection

Set the required environment variables before running dbt.

```bash
export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<your-token>"
```

Update `profiles.yml` with your Databricks connection details.

> Do not commit completed `profiles.yml` files, access tokens, passwords, or credentials to GitHub.

### Run the pipeline

```bash
dbt deps
dbt seed
dbt snapshot
dbt run
dbt test
```

---

## Project Structure

```text
.
├── seeds/                    # Raw e-commerce CSV files loaded to Bronze
├── models/
│   ├── staging/              # Standardized Silver models: stg_*
│   ├── intermediate/         # Enriched Silver models: int_*
│   └── marts/
│       └── core/             # Gold facts, dimensions, and exposures
├── snapshots/                # scd_customers Type 2 customer history
├── tests/                    # Custom singular data-quality tests
├── macros/                   # Reusable dbt macros and schema configuration
├── resources/                # Databricks Asset Bundle job definitions
├── .github/
│   └── workflows/            # GitHub Actions CI/CD workflows
├── docs/
│   └── images/
│       └── shopflow-star-schema.jpg
├── dbt_project.yml           # dbt project configuration and hooks
├── profiles.yml.example      # Connection-profile template
├── databricks.yml            # Databricks Asset Bundle configuration
├── requirements.txt          # Python and dbt dependencies
└── README.md
```

---

## License

See [LICENSE](LICENSE).
