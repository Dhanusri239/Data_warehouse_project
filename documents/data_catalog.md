# 📘 Enterprise Data Warehouse – Data Catalog

---

# 1️⃣ Project Overview

This data warehouse follows a **Medallion Architecture**:

- 🥉 Bronze → Raw ingestion layer  
- 🥈 Silver → Cleaned & standardized layer  
- 🥇 Gold → Business-ready analytical layer  

The warehouse is designed using **Dimensional Modeling (Star Schema)** principles.

---

# 2️⃣ Architecture Overview

Source Systems  
⬇  
Bronze (Raw Landing)  
⬇  
Silver (Cleaned & Standardized)  
⬇  
Gold (Star Schema for BI & Analytics)

---

# 3️⃣ Naming Conventions

| Prefix | Meaning |
|--------|---------|
| cst_   | Customer related column |
| prd_   | Product related column |
| dim_   | Dimension table |
| fact_  | Fact table |
| dwh_   | Data warehouse metadata column |

---

# 🥉 Bronze Layer

## bronze.crm_cust_info

**Purpose:** Raw customer data ingestion  
**Grain:** One row per customer record from source  

| Column Name         | Data Type      | Null     | Description |
|---------------------|---------------|----------|-------------|
| cst_id              | INT           | NOT NULL | Source customer ID |
| cst_key             | VARCHAR(50)   | NULL     | Business customer key |
| cst_firstname       | VARCHAR(100)  | NULL     | Raw first name |
| cst_lastname        | VARCHAR(100)  | NULL     | Raw last name |
| cst_marital_status  | VARCHAR(20)   | NULL     | Raw marital status |
| cst_gndr            | VARCHAR(10)   | NULL     | Raw gender |
| cst_create_date     | DATE          | NULL     | Record creation date |

---

## bronze.crm_prd_info

**Purpose:** Raw product data ingestion  
**Grain:** One row per product record from source  

| Column Name   | Data Type       | Null     | Description |
|---------------|----------------|----------|-------------|
| prd_id        | INT            | NOT NULL | Source product ID |
| prd_key       | VARCHAR(50)    | NULL     | Business product key |
| prd_nm        | VARCHAR(200)   | NULL     | Raw product name |
| prd_cost      | DECIMAL(10,2)  | NULL     | Raw product cost |
| prd_line      | VARCHAR(50)    | NULL     | Raw product line |
| prd_start_dt  | DATE           | NULL     | Product start date |
| prd_end_dt    | DATE           | NULL     | Product end date |

---

# 🥈 Silver Layer

## silver.crm_cust_info

**Purpose:** Cleaned & standardized customer data  
**Grain:** One row per unique customer  

| Column Name         | Data Type      | Null     | Description |
|---------------------|---------------|----------|-------------|
| cst_id              | INT           | NOT NULL | Source customer ID |
| cst_key             | VARCHAR(50)   | NOT NULL | Business key |
| cst_firstname       | VARCHAR(100)  | NOT NULL | Trimmed first name |
| cst_lastname        | VARCHAR(100)  | NOT NULL | Trimmed last name |
| cst_marital_status  | VARCHAR(20)   | NOT NULL | Standardized marital status |
| cst_gndr            | VARCHAR(10)   | NOT NULL | Standardized gender |
| cst_create_date     | DATE          | NOT NULL | Record creation date |
| dwh_create_date     | DATETIME      | NOT NULL | ETL load timestamp |

---

## silver.crm_prd_info

**Purpose:** Cleaned & standardized product data  
**Grain:** One row per unique product  

| Column Name   | Data Type       | Null     | Description |
|---------------|----------------|----------|-------------|
| prd_id        | INT            | NOT NULL | Source product ID |
| cat_id        | VARCHAR(20)    | NOT NULL | Extracted category ID |
| prd_key       | VARCHAR(50)    | NOT NULL | Business key |
| prd_nm        | VARCHAR(200)   | NOT NULL | Cleaned product name |
| prd_cost      | DECIMAL(10,2)  | NOT NULL | Cleaned product cost |
| prd_line      | VARCHAR(50)    | NOT NULL | Normalized product line |
| prd_start_dt  | DATE           | NOT NULL | Product start date |
| prd_end_dt    | DATE           | NULL     | Product end date (SCD handling) |

---

# 🥇 Gold Layer (Star Schema)

## gold.fact_sales

**Purpose:** Stores transactional sales data  
**Grain:** One row per sales transaction  

| Column Name   | Data Type        | Null      | Description |
|---------------|------------------|-----------|-------------|
| sales_key     | INT IDENTITY     | NOT NULL  | Surrogate primary key |
| order_number  | VARCHAR(50)      | NOT NULL  | Order ID |
| product_key   | INT              | NOT NULL  | FK → gold.dim_products |
| customer_key  | INT              | NOT NULL  | FK → gold.dim_customer |
| order_date    | DATE             | NOT NULL  | Order date |
| shipping_date | DATE             | NULL      | Shipping date |
| due_date      | DATE             | NULL      | Due date |
| sales_amount  | DECIMAL(12,2)    | NOT NULL  | Total sales |
| quantity      | INT              | NOT NULL  | Units sold |
| price         | DECIMAL(10,2)    | NOT NULL  | Unit price |

---

## gold.dim_customer

**Purpose:** Customer dimension  
**Grain:** One row per customer  

| Column Name     | Data Type        | Null      | Description |
|-----------------|------------------|-----------|-------------|
| customer_key    | INT              | NOT NULL  | Surrogate primary key |
| customer_id     | INT              | NOT NULL  | Source customer ID |
| customer_number | VARCHAR(50)      | NOT NULL  | Business key |
| first_name      | VARCHAR(100)     | NOT NULL  | First name |
| last_name       | VARCHAR(100)     | NOT NULL  | Last name |
| country         | VARCHAR(50)      | NOT NULL  | Customer country |
| marital_status  | VARCHAR(20)      | NOT NULL  | Marital status |
| gender          | VARCHAR(10)      | NOT NULL  | Gender |
| birth_date      | DATE             | NULL      | Date of birth |
| create_date     | DATE             | NOT NULL  | Account creation date |

---

## gold.dim_products

**Purpose:** Product dimension  
**Grain:** One row per product  

| Column Name     | Data Type        | Null      | Description |
|-----------------|------------------|-----------|-------------|
| product_key     | INT              | NOT NULL  | Surrogate primary key |
| product_id      | INT              | NOT NULL  | Source product ID |
| product_number  | VARCHAR(50)      | NOT NULL  | Business key |
| product_name    | VARCHAR(200)     | NOT NULL  | Product name |
| category_id     | VARCHAR(20)      | NOT NULL  | Category ID |
| category        | VARCHAR(100)     | NOT NULL  | Product category |
| subcategory     | VARCHAR(100)     | NOT NULL  | Product subcategory |
| maintenance     | VARCHAR(50)      | NULL      | Maintenance classification |
| cost            | DECIMAL(10,2)    | NOT NULL  | Product cost |
| product_line    | VARCHAR(50)      | NOT NULL  | Product line |
| start_date      | DATE             | NOT NULL  | Active start date |

---

# 4️⃣ Keys & Relationships

- gold.fact_sales.product_key → gold.dim_products.product_key  
- gold.fact_sales.customer_key → gold.dim_customer.customer_key  

Primary Keys:
- fact_sales → sales_key  
- dim_customer → customer_key  
- dim_products → product_key  

---

# 5️⃣ Slowly Changing Dimensions (SCD)

| Table | SCD Type |
|-------|----------|
| dim_customer | Type 1 |
| dim_products | Type 2 (if historical tracking enabled) |

---

# 6️⃣ Data Lineage

Source System  
→ Bronze Layer (Raw)  
→ Silver Layer (Standardized)  
→ Gold Layer (Analytical Star Schema)  
→ BI / Reporting Tools  

---

# 7️⃣ Indexing Strategy

- Clustered index on surrogate keys  
- Non-clustered indexes on foreign keys in fact table  
- Index on order_date for reporting performance  

---

# 8️⃣ Data Quality Controls

- Null validation in Silver layer  
- Standardization of gender and marital status  
- Duplicate removal based on business keys  
- Surrogate key enforcement in Gold layer  

---
