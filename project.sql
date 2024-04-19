-- Nhập dữ liệu vào PostgreSQL
CREATE TABLE customer_shopping_data
  (invoice_no BIGINT,
   customer_id BIGINT,
   category VARCHAR,
   quantity INT,
   price NUMERIC,
   payment_method VARCHAR,
   invoice_date VARCHAR,
   shopping_mall VARCHAR)

-- Import Data from customer_shopping_data.csv 

-- Làm sạch dữ liệu
  -- Thay đổi DATE FORMAT
ALTER TABLE public.customer_shopping_data
  ALTER COLUMN invoice_date TYPE DATE USING (TRIM(invoice_date):: DATE)

  -- Tìm và xóa các dòng dữ liệu trùng lặp
DELETE FROM public.customer_shopping_data 
WHERE invoice_no IN 
  (WITH duplicate AS	
  	(SELECT 
     ROW_NUMBER () OVER (PARTITION BY customer_id,category,quantity,price,payment_method,invoice_date,shopping_mall 
     ORDER BY invoice_no)AS stt, * 
     FROM public.customer_shopping_data)
  SELECT invoice_no FROM duplicate
  WHERE stt>1)

  -- Tìm và xử lý các giá trị ngoại lai
WITH pct AS
  (SELECT 
  percentile_cont (0.25) WITHIN GROUP (ORDER BY quantity ) AS Q1,
  percentile_cont (0.75) WITHIN GROUP (ORDER BY quantity ) AS Q3,
  percentile_cont (0.75) WITHIN GROUP (ORDER BY quantity ) - percentile_cont (0.25) WITHIN GROUP (ORDER BY quantity ) AS IQR
  FROM customer_shopping_data),
  
min_max AS
  (SELECT Q1-1.5*IQR AS min_value, Q3+1.5*IQR AS max_value FROM pct),
  outlier AS
  (SELECT * FROM customer_shopping_data 
  WHERE quantity < (SELECT min_value FROM min_max ) 
  OR quantity > (SELECT max_value FROM min_max))
  
UPDATE customer_shopping_data
SET quantity= (SELECT CAST(AVG(quantity) AS INT) FROM customer_shopping_data)
WHERE quantity IN (SELECT quantity FROM outlier)

-- Phân tích dữ liệu
  -- Số lượng đơn hàng, khách hàng và doanh thu hàng mỗi tháng
ALTER TABLE public.customer_shopping_data
	ADD COLUMN invoice_month INT;
ALTER TABLE public.customer_shopping_data
	ADD COLUMN invoice_year INT;

UPDATE public.customer_shopping_data
	SET invoice_month = EXTRACT (MONTH FROM invoice_date);
UPDATE public.customer_shopping_data
	SET invoice_year = EXTRACT (YEAR FROM invoice_date);

SELECT 
invoice_year, invoice_month,
COUNT (invoice_no) AS order_count,
COUNT (DISTINCT customer_id) AS customer_count,
SUM (quantity*price) AS revenue
FROM public.customer_shopping_data
GROUP BY invoice_year, invoice_month
ORDER BY invoice_year, invoice_month

  -- Top 3 sản phẩm bán chạy mỗi năm
WITH order_ranking AS	
	(SELECT 
	invoice_year, category,
	SUM(quantity) AS order_count,
	RANK () OVER (PARTITION BY invoice_year ORDER BY SUM(quantity) DESC ) AS stt
	FROM public.customer_shopping_data
	GROUP BY invoice_year, category
	ORDER BY invoice_year, SUM(quantity) DESC)
SELECT invoice_year, category
FROM order_ranking
WHERE stt<=3
-- Nhận xét: Food & Beverage, Clothing, Cosmetics là 3 sản phẩm bán chạy nhất trong cả 3 năm 2021,2022,2023.

  -- Cohort analysis
WITH first_purchase_date AS	
	(SELECT 
	customer_id,
	quantity*price AS amount,
	MIN(invoice_date) OVER(PARTITION BY customer_id ) AS first_date,
	invoice_date
	FROM public.customer_shopping_data)
SELECT customer_id, amount,
TO_CHAR (first_date,'YYYY/MM') AS cohort_date,
invoice_date,
(EXTRACT(YEAR FROM invoice_date) - EXTRACT (YEAR FROM first_date))*12 + EXTRACT(MONTH FROM invoice_date) - 
EXTRACT (MONTH FROM first_date) +1  AS index
FROM first_purchase_date
  -- Ai là khách hàng tốt nhất, phân tích dựa vào RFM 



