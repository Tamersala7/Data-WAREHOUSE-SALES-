/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
if object_id ('BRONZE.crm_cust_info','U') IS NOT NULL 
	DROP TABLE BRONZE.crm_cust_info
Go
CREATE TABLE BRONZE.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
)
Go
  
if object_id ('BRONZE.crm_prd_info','U') IS NOT NULL 
	DROP TABLE BRONZE.crm_prd_info
Go
CREATE TABLE BRONZE.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
)
Go
  
if object_id ('BRONZE.crm_sales_details','U') IS NOT NULL 
	DROP TABLE BRONZE.crm_sales_details
Go
CREATE TABLE BRONZE.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)
Go
if object_id ('BRONZE.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE BRONZE.erp_loc_a101
Go
CREATE TABLE BRONZE.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)

)
Go
if object_id ('BRONZE.erp_cust_az12','U') IS NOT NULL 
	DROP TABLE BRONZE.erp_cust_az12
Go
CREATE TABLE BRONZE.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
)
Go
if object_id ('BRONZE.erp_ex_cat_g1v2','U') IS NOT NULL 
	DROP TABLE BRONZE.erp_ex_cat_g1v2
Go
CREATE TABLE BRONZE.erp_ex_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
ssubcat nvarchar(50),
maintenance nvarchar(50),
)
GO
