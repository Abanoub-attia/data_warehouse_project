USE DataWarehouse;
GO

------------------------------------------------------------
--building the customer dimension table in the golden layer
------------------------------------------------------------
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.dim_customer;
END;
GO
-- create an object for the custemer dimension
CREATE VIEW gold.dim_customer AS
SELECT
    --rename the columns to friendly, meaningful names
    --sort the columns into logical order to improve readability
    --generate a primary key (surrogate key) for the customer dimension table
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, --query based surrogate key generation
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    --we have to soureces for the gender so we assume that CRM has more accurate data 
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ci.cst_marital_status AS marital_status,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la ON ci.cst_key = la.cid;
------------------------------------------------------------
--building the product dimension table in the golden layer
------------------------------------------------------------
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.dim_product;
END;
GO
-- create an object for the product dimension
CREATE VIEW gold.dim_product AS
SELECT
    --rename the columns to friendly, meaningful names
    --sort the columns into logical order to improve readability
    --generate a primary key (surrogate key) for the customer dimension table
    ROW_NUMBER() OVER (ORDER BY pi.prd_id) AS product_key, --query based surrogate key generation
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
	--pi.prd_end_dt, --it always a NULL
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pi.cat_id = pc.id
--we have historical data in the ERP system for the product category, so we use only the current data
WHERE pi.prd_end_dt IS NULL;
------------------------------------------------------------
--building the fact sales table in the golden layer
------------------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
BEGIN
    DROP VIEW gold.fact_sales;
END;
GO
-- create an object for the sales fact
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key,
    --to connect the fact with the dims 
    cu.customer_key,
    --to connect the fact with the dims 
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customer AS cu ON cu.customer_id = sd.sls_cust_id
LEFT JOIN gold.dim_product AS pr ON pr.product_number = sd.sls_prd_key;
