/*=========================================================================================================================
🥇 GOLD LAYER — Data Quality Check
===========================================================================================================================*/

--1️⃣ gold.fact_sales
--✅ Check 1: Foreign Key Validation – Customer
/*
Purpose: Ensure all customer_key values in fact table
exist in dim_customer.
*/

SELECT fs.customer_key
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customer dc
    ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;
--======================================================================================================================
--✅ Check 2: Foreign Key Validation – Product
/*
Purpose: Ensure all product_key values in fact table
exist in dim_products.
*/
SELECT fs.product_key
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
    ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;
--===========================================================================================================================
--✅ Check 3: Negative Sales Amount
/*
Purpose: Sales amount should never be negative.
*/

SELECT *
FROM gold.fact_sales
WHERE sales_amount < 0;
--===========================================================================================================================
--✅ Check 4: Sales Amount Consistency Check
/*
Purpose: Validate sales_amount = quantity * price.
Allowing small rounding tolerance.
*/

SELECT *
FROM gold.fact_sales
WHERE ABS(sales_amount - (quantity * price)) > 0.01;
--===========================================================================================================================
--2️⃣ gold.dim_customer
--✅ Check 1: Surrogate Key Uniqueness
/*
Purpose: Ensure surrogate key uniqueness.
*/

SELECT customer_key, COUNT(*)
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;
--===========================================================================================================================
--✅ Check 2: Null critical attributes
/*
Purpose: Ensure essential dimension attributes are populated.
*/

SELECT *
FROM gold.dim_customer
WHERE customer_id IS NULL
   OR first_name IS NULL
   OR last_name IS NULL;
--===========================================================================================================================
--3️⃣ gold.dim_products
--✅ Check 1: Surrogate Key Uniqueness
/*
Purpose: Ensure surrogate key uniqueness.
*/

SELECT product_key, COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
--===========================================================================================================================
--✅ Check 2: Cost validation
/*
Purpose: Ensure product cost is positive.
*/

SELECT *
FROM gold.dim_products
WHERE cost <= 0;
--===========================================================================================================================
