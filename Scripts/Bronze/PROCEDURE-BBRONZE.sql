/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
begin
	DECLARE @start_time DATETIME ,@end_time DATETIME,@batch_start_time datetime, @batch_end_time datetime;
	
	begin try
		SET @batch_start_time =GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================';
	
		print '----------------------------------------------';
		print ' Loading CRM Tables';
		print '----------------------------------------------';

		SET @start_time =GETDATE();
		print '>> Truncating Table: BRONZE.CRM_CUST_INFO';
		Truncate table BRONZE.CRM_CUST_INFO;

		print '>> Inserting Table Into: BRONZE.CRM_CUST_INFO';
		BULK INSERT BRONZE.CRM_CUST_INFO
		FROM 'C:\Users\tamer\OneDrive\Desktop\dwh\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		print'----------------------------------'
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print'----------------------------------'
		
		SET @start_time =GETDATE();
		print '>> Truncating Table: BRONZE.crm_prd_info'
		Truncate table BRONZE.crm_prd_info

		print '>> Inserting Table Into: BRONZE.crm_prd_info'
		BULK INSERT BRONZE.crm_prd_info
		FROM 'C:\Users\tamer\OneDrive\Desktop\dwh\source_crm\prd_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		print'----------------------------------'
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print'----------------------------------'

		SET @start_time =GETDATE();
		print '>> Truncating Table: BRONZE.crm_sales_details'
		Truncate table BRONZE.crm_sales_details

		print '>> Inserting Table Into: BRONZE.crm_sales_details'
		BULK INSERT BRONZE.crm_sales_details
		FROM 'C:\Users\tamer\OneDrive\Desktop\dwh\source_crm\sales_details.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		print'----------------------------------'
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print'----------------------------------'

		SET @start_time =GETDATE();
		print '>> Truncating Table: BRONZE.erp_cust_az12'
		Truncate table BRONZE.erp_cust_az12

		print '>> Inserting Table Into: BRONZE.erp_cust_az12'
		BULK INSERT BRONZE.erp_cust_az12
		FROM 'C:\Users\tamer\OneDrive\Desktop\dwh\source_erp\CUST_AZ12.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		print'----------------------------------'
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print'----------------------------------'

		SET @start_time =GETDATE();
		print '>> Truncating Table: BRONZE.erp_loc_a101'
		Truncate table BRONZE.erp_loc_a101

		print '>> Inserting Table Into: BRONZE.erp_loc_a101'
		BULK INSERT BRONZE.erp_loc_a101
		FROM 'C:\Users\tamer\OneDrive\Desktop\dwh\source_erp\LOC_A101.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		print'----------------------------------'
		PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		print'----------------------------------'

		SET @start_time =GETDATE();
		print '>> Truncating Table: BRONZE.erp_ex_cat_g1v2'
		Truncate table BRONZE.erp_ex_cat_g1v2

		print '>> Inserting Table Into: BRONZE.erp_ex_cat_g1v2'
		BULK INSERT BRONZE.erp_ex_cat_g1v2
		FROM 'C:\Users\tamer\OneDrive\Desktop\dwh\source_erp\PX_CAT_G1V2.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @batch_end_time =GETDATE();
	PRINT '>> Total Load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	end try
	
	begin catch 
		print '===================================='
		print 'ERROR OCCUERD DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		print '===================================='
	end catch 
end
