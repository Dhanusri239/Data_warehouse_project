--================================================================================================================
--🥈 SILVER LAYER — Data Quality Checks
--================================================================================================================
--1️⃣ silver.crm_cust_info
--✅ Check 1: Null validation for mandatory columns
--Purpose: Ensure critical business fields are not NULL before promotion to Gold layer.
-------------------------------------------------------------------------------------------------------------------
/*
Purpose: Validate that mandatory customer fields contain no NULL values.
If any rows return, data quality issue exists.
*/

SELECT *
FROM silver.crm_cust_info
WHERE cst_id IS NULL
   OR cst_key IS NULL
   OR cst_firstname IS NULL
   OR cst_lastname IS NULL
   OR cst_marital_status IS NULL
   OR cst_gndr IS NULL
   OR cst_create_date IS NULL;
--====================================================================================================================
--✅ Check 2: Duplicate business keys
--Purpose: Ensure one record per unique customer business key.
--===================================================================================================================
/*
Purpose: Detect duplicate customer business keys.
Each customer must be unique in Silver layer.
*/

SELECT cst_key, COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_key
HAVING COUNT(*) > 1;
--========================================================================================================================
--✅ Check 3: Valid Gender Standardization
--Purpose: Ensure gender is standardized (e.g., M / F).
--========================================================================================================================
/*
Purpose: Validate gender normalization.
Only allowed values: 'M', 'F'
*/

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('M', 'F');
--========================================================================================================================
--✅ Check 4: Valid Marital Status
/*
Purpose: Validate marital status normalization.
Allowed values: 'Single', 'Married'
*/
--========================================================================================================================
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single', 'Married');
--========================================================================================================================
--2️⃣ silver.crm_prd_info
--✅ Check 1: Null validation
/*
Purpose: Ensure required product attributes are not NULL.
*/
--========================================================================================================================
SELECT *
FROM silver.crm_prd_info
WHERE prd_id IS NULL
   OR prd_key IS NULL
   OR prd_nm IS NULL
   OR prd_cost IS NULL
   OR prd_line IS NULL
   OR prd_start_dt IS NULL;
--========================================================================================================================
--✅ Check 2: Negative or zero product cost
/*
Purpose: Ensure product cost is valid and positive.
*/
--========================================================================================================================
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost <= 0;
--========================================================================================================================
--✅ Check 3: Duplicate product keys
/*
Purpose: Ensure product business key uniqueness.
*/
--========================================================================================================================
SELECT prd_key, COUNT(*) AS record_count
FROM silver.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1;
