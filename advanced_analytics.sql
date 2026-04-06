USE DataWarehouse;
GO
------------------------------
--Change over Time Analysis
------------------------------
--Analyze sales performance over time
SELECT
    DATETRUNC(month, fs.order_date) AS order_date, 
    SUM(fs.sales_amount) AS total_sales_per_order_date,
    COUNT(DISTINCT fs.customer_key) AS number_of_customer_per_year,
    SUM(fs.quantity) AS number_of_sold_items_per_year
FROM gold.fact_sales AS fs
WHERE fs.order_date IS NOT NULL
GROUP BY DATETRUNC(month, fs.order_date)
ORDER BY DATETRUNC(month, fs.order_date) ASC;
-------------------------------
SELECT
    FORMAT(fs.order_date, 'yyyy-MMM') AS order_date,
    SUM(fs.sales_amount) AS total_sales_per_order_date,
    COUNT(DISTINCT fs.customer_key) AS number_of_customer_per_year,
    SUM(fs.quantity) AS number_of_sold_items_per_year
FROM gold.fact_sales AS fs
WHERE fs.order_date IS NOT NULL
GROUP BY FORMAT(fs.order_date, 'yyyy-MMM')
--will not be sorted probably bacause FORMAT() return string 
ORDER BY FORMAT(fs.order_date, 'yyyy-MMM') ASC;
------------------------------
--Cumulative Analysis
------------------------------
--Calculate the total sales per month
--Calculate the running total of sales over time
SELECT
    order_date,
    total_sales_per_order_date,
    SUM(total_sales_per_order_date) OVER(PARTITION BY YEAR(order_date) ORDER BY order_date) AS running_total_sales_per_order_date
FROM    
    (SELECT
        DATETRUNC(month, fs.order_date) AS order_date,
        SUM(fs.sales_amount) AS total_sales_per_order_date
    FROM gold.fact_sales AS fs
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(month, fs.order_date)) AS subquery;
-------------------------------
SELECT
    order_date,
    total_sales_per_order_date,
    SUM(total_sales_per_order_date) OVER(ORDER BY order_date) AS running_total_sales_per_order_date,
    AVG(average_price_order_date) OVER(ORDER BY order_date) AS moving_average_sales_per_order_date
FROM    
    (SELECT
        DATETRUNC(year, fs.order_date) AS order_date,
        SUM(fs.sales_amount) AS total_sales_per_order_date,
        AVG(fs.price) AS average_price_order_date
    FROM gold.fact_sales AS fs
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNSELECT
    (total_sales_per_order_date - previos_year_sales),
    (total_sales_per_order_date - average_sales)
FROM
    (SELECT
        order_date,
        total_sales_per_order_date,
        LAG(total_sales_per_order_date) OVER(ORDER BY order_date) previos_year_sales,
        AVG(total_sales_per_order_date) OVER() average_sales
    FROM    
        (SELECT
            DATETRUNC(year, fs.order_date) AS order_date,
            SUM(fs.sales_amount) AS total_sales_per_order_date
        FROM gold.fact_sales AS fs
        WHERE order_date IS NOT NULL
        GROUP BY DATETRUNC(year, fs.order_date)) AS subquery)C(year, fs.order_date)) AS subquery;
------------------------------
--Performance Analysis
------------------------------
/*Analyze the yearly performance of products by comparing their sales to both
the average sales performance of the product and the previous year's sales*/
SELECT
    (total_sales_per_order_date - previos_year_sales),
    (total_sales_per_order_date - average_sales)
FROM
    (SELECT
        order_date,
        total_sales_per_order_date,
        LAG(total_sales_per_order_date) OVER(ORDER BY order_date) previos_year_sales,
        AVG(total_sales_per_order_date) OVER() average_sales
    FROM    
        (SELECT
            DATETRUNC(year, fs.order_date) AS order_date,
            SUM(fs.sales_amount) AS total_sales_per_order_date
        FROM gold.fact_sales AS fs
        WHERE order_date IS NOT NULL
        GROUP BY DATETRUNC(year, fs.order_date)) AS subquery):
