/*==========================================================================================================================
Clean and Insert Data bronze.crm into silver.crm
Note:The Purpose of this file is to handle and cleaning up the data so the data may be altered if it is already existed
in your database
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_starting_time DATETIME,@batch_ending_time DATETIME;
    BEGIN TRY
	    SET @batch_starting_time=GETDATE();
		PRINT'============================================================================================';
		PRINT'Loading silver Layer '
		PRINT'============================================================================================';

		PRINT'--------------------------------------------------------------------------------------------';
		PRINT'Loading CRM Table'
		PRINT'--------------------------------------------------------------------------------------------';
		--==========================================================================================================================
		--Cleaning up the messy datas and Inserting the cleaned data in the table silver.crm_cust_info
		--==========================================================================================================================
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLES:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info

		INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
		SELECT
		cst_id,
		cst_key,
		--cleaning up the extra white spaces
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		--Normalize the marital status to the readable format
		CASE WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'
			 ELSE 'n/a' 
		END AS cst_marital_status,
		--Normalize the gender values to the readable format
		CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			 ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM(SELECT *,
		--select the most recent record per customer
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC)AS flag
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t 
		WHERE flag=1
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------';
		--==========================================================================================================================
		--Cleaning up the messy datas and Inserting the cleaned data in the table silver.crm_prd_info
		--==========================================================================================================================
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLES:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info

		INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		SELECT
		prd_id,
		--Extract category ID
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		--Extract category Key
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		--Handling NULL
		COALESCE(prd_cost,0) AS prd_cost,
		--Normalize product line code
		CASE UPPER(TRIM(prd_line)) 
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		--calculate end date as one day before the next start date
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------';

		--==========================================================================================================================
		--Cleaning up the messy datas and Inserting the cleaned data in the table silver.crm_sales_details
		--==========================================================================================================================
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLES:silver.crm_sls_details';
		TRUNCATE TABLE silver.crm_sls_details

		INSERT INTO silver.crm_sls_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,
		sls_quantity,sls_price)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		--Handling Invalid data
		CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt ) !=8 THEN NULL
		ELSE CAST(CAST(sls_due_dt  AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		--Recalculate sales if original values are missing
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!=sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		--Recalculate Price if it is Invalid
		CASE WHEN sls_price IS NULL OR sls_price<=0 THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sls_details
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------';
		PRINT'--------------------------------------------------------------------------------------------';
		PRINT'Loading CRM Table is completed'
		PRINT'--------------------------------------------------------------------------------------------';
		--=========================================================================================================================
		--Clean and Insert data from bronze.erp into silver.erp
		--=========================================================================================================================

		--==========================================================================================================================
		--Cleaning up the messy datas and Inserting the cleaned data in the table silver.erp_cust_az12
		--==========================================================================================================================
		PRINT'--------------------------------------------------------------------------------------------';
		PRINT'Loading ERP Table'
		PRINT'--------------------------------------------------------------------------------------------';
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLES:silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12

		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		SELECT
		--cleaning up additional content in cid
		CASE WHEN cid  LIKE 'NAS%' THEN  SUBSTRING(cid,4,LEN(cid))
		ELSE cid
		END AS cid,
		--Normalizing date to NULL if it's in future
		CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
		END AS bdate,
		--Normalizing gender in easy readable format
		CASE 
		WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
		ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------';
		--==========================================================================================================================
		--Cleaning up the messy datas and Inserting the cleaned data in the table silver.erp_loc_a101
		--==========================================================================================================================
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLES:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101

		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT
		--Cleaning up the id if it has special character
		REPLACE(cid,'-','')cid,
		--Normalizing the country in easy readable format
		CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
		WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
		WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------';
		--==========================================================================================================================
		--Cleaning up the messy datas and Inserting the cleaned data in the table silver.erp_px_cat_g1v2 
		--==========================================================================================================================
		SET @start_time=GETDATE();
		PRINT '>>TRUNCATING TABLES:silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2

		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2 
		SET @end_time=GETDATE();

		PRINT'--------------------------------------------------------------------------------------------';
		PRINT'Loading ERP Table is completed'
		PRINT'--------------------------------------------------------------------------------------------';
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------';
		SET @batch_ending_time=GETDATE();
		PRINT'======================================================================================================';
		PRINT'Loading silver layer is completed'
		PRINT'       -Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_starting_time,@batch_ending_time) AS NVARCHAR)+' seconds';
		PRINT'======================================================================================================';
		END TRY
		BEGIN CATCH
		 PRINT'====================================================================================================';
			 PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
			 PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
			 PRINT'ERROR NUMBER'+CAST(ERROR_NUMBER() AS NVARCHAR);
			 PRINT'ERROR STATE'+CAST(ERROR_STATE() AS NVARCHAR);
			 PRINT'================================================================================================';
		END CATCH
END
--Executing stored procedure silver.load_silver
EXEC silver.load_silver
