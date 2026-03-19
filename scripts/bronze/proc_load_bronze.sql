/*
===============================================================================
Stored Procedure: bronze.load_bronze (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses BULK INSERT to load data from CSV files into bronze tables.

Parameters:
    None

Returns:
    None

Usage Example:
    EXEC bronze.load_bronze;

===============================================================================
*/

CREATE OR ALTER PROCEDURE BRONZE.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY

	SET @batch_start_time= GETDATE();

		PRINT('============================================================');
		PRINT('BRONZE LEVEL');
		PRINT('============================================================');

		PRINT('-------------------------------------------------------------');
		PRINT('LOADING CRM TABLES');
		PRINT('-------------------------------------------------------------');


		SET @start_time=GETDATE();

		TRUNCATE TABLE BRONZE.crm_cust_info;
		PRINT('INSERTING INTO THE TABLE : BRONZE.crm_cust_info');
		BULK INSERT BRONZE.crm_cust_info
		FROM 'D:\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time = GETDATE ();
		PRINT('>>>> LOAD DURATION '+CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds');



		SELECT COUNT(*) FROM BRONZE.crm_cust_info;

		SET @start_time=GETDATE();

		TRUNCATE TABLE BRONZE.crm_prd_info;
		PRINT('INSERTING INTO THE TABLE : BRONZE.crm_prd_info');
		BULK INSERT BRONZE.crm_prd_info
		FROM 'D:\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time = GETDATE ();
		PRINT('>>>> LOAD DURATION '+CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds');

		SELECT COUNT(*) FROM BRONZE.crm_prd_info;

		SET @start_time=GETDATE();


		TRUNCATE TABLE BRONZE.crm_sales_details;
		PRINT('INSERTING INTO THE TABLE : BRONZE.crm_sales_details');
		BULK INSERT BRONZE.crm_sales_details
		FROM 'D:\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time = GETDATE ();
		PRINT('>>>> LOAD DURATION '+CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds');

		SELECT COUNT(*) FROM BRONZE.crm_sales_details;

		PRINT('-------------------------------------------------------------');
		PRINT('LOADING ERP TABLES');
		PRINT('-------------------------------------------------------------');

		SET @start_time=GETDATE();

		TRUNCATE TABLE BRONZE.erp_cust_az12;
		PRINT('INSERTING INTO THE TABLE : BRONZE.erp_cust_az12');
		BULK INSERT BRONZE.erp_cust_az12
		FROM 'D:\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time = GETDATE ();
		PRINT('>>>> LOAD DURATION '+CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds');

		SELECT COUNT(*) FROM BRONZE.erp_cust_az12;


		SET @start_time=GETDATE();

		TRUNCATE TABLE BRONZE.erp_loc_a101;
		PRINT('INSERTING INTO THE TABLE : BRONZE.erp_loc_a101');
		BULK INSERT BRONZE.erp_loc_a101
		FROM 'D:\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time = GETDATE ();
		PRINT('>>>> LOAD DURATION '+CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds');

		SELECT COUNT(*) FROM BRONZE.erp_loc_a101;

		SET @start_time=GETDATE();


		TRUNCATE TABLE BRONZE.erp_px_cat_g1v2;
		PRINT('INSERTING INTO THE TABLE : BRONZE.erp_px_cat_g1v2');
		BULK INSERT BRONZE.erp_px_cat_g1v2
		FROM 'D:\Downloads\dbc9660c89a3480fa5eb9bae464d6c07 (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time = GETDATE ();
		PRINT('>>>> LOAD DURATION '+CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds');

		SELECT COUNT(*) FROM BRONZE.erp_loc_a101;

		SET @batch_end_time= GETDATE();
		PRINT('=====================================================================');
		PRINT('LOADING OF THE BRONZE LAYER IS COMPLETED');
		PRINT('=====================================================================');
		PRINT('DURATION OF LOADING WHOLE BRONZE LAYER IS');
		PRINT(CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +'seconds');
		PRINT('=====================================================================');


  END TRY
BEGIN CATCH
	PRINT('=============================================================');
	PRINT('ERROR OCCURED DURING BRONZE LAYER');
	PRINT('=============================================================');
	PRINT('ERROR MESSAGE + ERROR_MESSAGE()');
	PRINT('ERROR NUMBER + CAST(ERROR_NUMBER() ) AS NVARCHAR');
END CATCH



END
