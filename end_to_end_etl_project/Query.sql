CREATE DATABASE end_to_end_project;
use end_to_end_project;
CREATE TABLE df_orders(
	order_id int,
	order_date date,
	ship_mode varchar(255),
	segment varchar(255),
	country varchar(255),
	city varchar(255),
	state varchar(255),
	postal_code int,
	region varchar(255),
	category varchar(255),
	sub_category varchar(255),
	product_id varchar(255),
	quantity int,
	discount float,
	sale_price float,
	profit float
);
SELECT
	*
FROM df_orders;
BULK INSERT df_orders
FROM 'E:\anaconda_data_engineering\envs\cloud_data_engineeringenv\end_to_end_etl_project\orders_updata.csv'
WITH
(
	FORMAT='CSV',
	FIRSTROW=2
)
GO
--TOP 10 PRODUCTS  BY HIGHEST REVENUE
SELECT TOP 10
	product_id,
	SUM(sale_price) AS sales
INTO top_product
FROM df_orders
GROUP BY
	product_id
ORDER BY 
	SUM(sale_price) DESC;
--TOP 5 REGION
WITH CTE AS(
SELECT 
	product_id,
	SUM(sale_price) AS sales,
	region
FROM df_orders
GROUP BY 
	product_id,
	region
)
SELECT *
INTO top_five_region
FROM (
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY region ORDER BY sales DESC) AS RANK
FROM CTE) A
WHERE RANK<=5;

--CAMPARE SALE OF EACH CORREPONDING MONTH OF PREVIOUS YAEAR 
WITH CTE AS (
SELECT 
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sale_price) AS sales
FROM df_orders
GROUP BY 
	YEAR(order_date),
	MONTH(order_date) 
)
SELECT
	order_month,
	SUM(CASE 
		WHEN order_year =2022 
		THEN sales 
		ELSE 0 
		END) AS SALES_2023,
	SUM(CASE 
		WHEN ORDER_YEAR=2023 
		THEN SALES 
		ELSE 0 
		END) AS SALES_2023
FROM CTE
GROUP BY
	order_month
ORDER BY
	order_month;

--CAMPARE SALE OF EACH CORREPONDING MONTH OF PREVIOUS YEAR  WITH RESPECT TO CATEGORY
WITH CTE AS(
SELECT
	category,
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	sum(sale_price) AS total_sales
FROM df_orders
GROUP BY
	category,
	YEAR(order_date),
	month(order_date)
)
SELECT
*
INTO sales_wrt_category
FROM CTE
ORDER BY
	category,
	order_month;
--SUMMARY TABLE
SELECT 
	category,
	SUM(sale_price) sales,
	SUM(profit) profit
INTO SUMMARY
FROM df_orders
GROUP BY
	CATEGORY
ORDER BY 
	sales DESC,
	profit DESC;

select * from SUMMARY