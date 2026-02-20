/*=============================================================================================================
The Purpose of this file is to insert the bulk amount of data from your local pc and to truncate the data if it
is already existed in our database table additionaly this file handles error by providing error message and line
in it.
Note:This code may delete the datas if it is existed in your database.
=============================================================================================================*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_starting_time DATETIME,@batch_ending_time DATETIME;	
    BEGIN TRY
	    SET @batch_starting_time=GETDATE();
		PRINT'=====================================================================================================';
		PRINT'Loading Bronze Layer'
		PRINT'=====================================================================================================';


		PRINT'-----------------------------------------------------------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'-----------------------------------------------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> INSERTING TABLE:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQLData\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> INSERATING TABLE:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQLData\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:bronze.crm_sls_details';
		TRUNCATE TABLE bronze.crm_sls_details ;

		PRINT'>> INSERTING TABLE:bronze.crm_sls_details';
		BULK INSERT bronze.crm_sls_details 
		FROM 'C:\SQLData\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------'

		PRINT'------------------------------------------------------------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'------------------------------------------------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101 ;

		PRINT'>> INSERTING TABLE:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101 
		FROM 'C:\SQLData\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------'

		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 ;

		PRINT'>> TRUNCATING TABLE:bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\SQLData\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------'
        
		SET @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE:bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;

		PRINT'>> INSERTING TABLE:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQLData\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'------------------'
		SET @batch_ending_time=GETDATE();
		PRINT'======================================================================================================';
		PRINT'Loading bronze layer is completed'
		PRINT'       -Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_starting_time,@batch_ending_time) AS NVARCHAR)+' seconds';
		PRINT'======================================================================================================';
	END TRY
	BEGIN CATCH
	         PRINT'================================================================================================';
			 PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
			 PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
			 PRINT'ERROR NUMBER'+CAST(ERROR_NUMBER() AS NVARCHAR);
			 PRINT'ERROR STATE'+CAST(ERROR_STATE() AS NVARCHAR);

			 PRINT'================================================================================================';
	END CATCH
END
--===================================================================================================================
--Executing the stored procedure
EXEC bronze.load_bronze
--===================================================================================================================


