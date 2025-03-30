/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver As
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=============================================';
        PRINT 'Loading SILVER Layer';
        PRINT '=============================================';

        PRINT '----------------------------------------------';
        PRINT ' Loading CRM Tables';
        PRINT '----------------------------------------------';

        -- Load CRM_CUST_INFO
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: SILVER.CRM_CUST_INFO';
        TRUNCATE TABLE SILVER.CRM_CUST_INFO;

        PRINT '>> Inserting Table Into: SILVER.CRM_CUST_INFO';
        INSERT INTO SILVER.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, 
            cst_material_status, cst_gndr, cst_create_date
        )
        SELECT cst_id, cst_key, 
            TRIM(cst_firstname) AS cst_firstname, 
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_material_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS DD
            FROM BRONZE.crm_cust_info
            WHERE CST_ID IS NOT NULL 
        ) subquery
        WHERE DD = 1;

        SET @end_time = GETDATE();
        PRINT '----------------------------------';
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '----------------------------------';

        -- Load CRM_PRD_INFO
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: SILVER.crm_prd_info';
        TRUNCATE TABLE SILVER.crm_prd_info;

        PRINT '>> Inserting Table Into: SILVER.crm_prd_info';
        INSERT INTO SILVER.crm_prd_info (
            prd_id, cat_id, PRD_KEY, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(PRD_KEY, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(PRD_KEY, 7, LEN(PRD_KEY) - 6) AS PRD_KEY,
            prd_nm,
            NULLIF(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'    
            END AS prd_line,
            prd_start_dt,
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
        FROM BRONZE.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '----------------------------------';
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '----------------------------------';

        -- Load ERP_CUST_AZ12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: SILVER.erp_cust_az12';
        TRUNCATE TABLE SILVER.erp_cust_az12;

        PRINT '>> Inserting Table Into: SILVER.erp_cust_az12';
        INSERT INTO SILVER.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid) - 3) ELSE cid END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM BRONZE.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '----------------------------------';
        PRINT '>> Load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '----------------------------------';

        -- Load ERP_LOC_A101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: SILVER.erp_loc_a101';
        TRUNCATE TABLE SILVER.erp_loc_a101;

        PRINT '>> Inserting Table Into: SILVER.erp_loc_a101';
        INSERT INTO SILVER.erp_loc_a101 (CID, cntry)
        SELECT  
            REPLACE(CID, '-', '') AS CID,
            CASE
                WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
                WHEN cntry = '' OR CNTRY IS NULL THEN 'N/A'
                ELSE TRIM(cntry)
            END AS cntry
        FROM BRONZE.erp_loc_a101;

        SET @batch_end_time = GETDATE();
        PRINT '----------------------------------';
        PRINT '>> Total Load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(50)) + ' seconds';
        PRINT '----------------------------------';

    END TRY
    BEGIN CATCH 
        PRINT '====================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR(50));
        PRINT '====================================';
    END CATCH 
END;
