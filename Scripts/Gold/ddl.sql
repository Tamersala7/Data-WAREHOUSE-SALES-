/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers as 
SELECT 
ROW_NUMBER() OVER (ORDER BY CU.CST_ID) AS Customer_Key,
	CU.cst_id as Customer_id,
	cu.cst_key as Customer_number,
	cu.cst_firstname as First_name,
	cu.cst_lastname as Last_name,
	cu.cst_material_status as Material_status,
	case
		when cu.cst_gndr != 'n/a' then cu.cst_gndr
		else coalesce(ca.gen,'n/a')
	end as Gender,
	ca.bdate as Birthdate,
	cl.cntry as Country,
	cu.cst_create_date as Create_date
from SILVER.crm_cust_info cu
left join SILVER.erp_cust_az12 ca
on cu.cst_key=ca.cid
left join SILVER.erp_loc_a101 cl
on cu.cst_key=cl.cid
GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as

select
	row_number() over(order by pr.prd_start_dt,pr.prd_key) as product_key,
	pr.prd_id as Product_id,
	pr.prd_key as Product_number,
	pr.prd_nm as Product_name,
	pr.cat_id as Categorey_id,
	ex.cat as Categorey,
	ex.ssubcat as SubCategorey,
	ex.maintenance as Maintenace,
	pr.prd_cost as Cost,
	pr.prd_line as product_line,
	pr.prd_start_dt as start_date
from SILVER.crm_prd_info as pr
left join SILVER.erp_ex_cat_g1v2 as ex
on pr.cat_id = ex.id
where prd_end_dt is null
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales as
select 
	s.sls_ord_num as Order_Number,
	p.product_key,
	c.Customer_Key,
	s.sls_order_dt AS Order_date,
	s.sls_ship_dt as Shipping_date,
	s.sls_due_dt as due_date,
	s.sls_sales as sales,
	s.sls_quantity as quantity,
	s.sls_price as Price
from SILVER.crm_sales_details s
left join GOLD.dim_products p
on s.sls_prd_key=p.Product_number
left join GOLD.dim_customers c
on s.sls_cust_id=c.Customer_id
GO
