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
CREATE VIEW vw_cohort AS	
	(WITH first_purchase_date AS	
		(SELECT 
		customer_id,
		quantity*price AS amount,
		MIN(invoice_date) OVER(PARTITION BY customer_id ) AS first_date,
		invoice_date
		FROM public.customer_shopping_data),
	cohort_index AS
		(SELECT customer_id, amount,
		TO_CHAR (first_date,'YYYY/MM') AS cohort_date,
		invoice_date,
		(EXTRACT(YEAR FROM invoice_date) - EXTRACT (YEAR FROM first_date))*12 + EXTRACT(MONTH FROM invoice_date) - 
		EXTRACT (MONTH FROM first_date) +1  AS index
		FROM first_purchase_date)
	SELECT cohort_date,
	index,
	COUNT (DISTINCT customer_id) AS user_count,
	SUM(amount) AS revenue
	FROM cohort_index 
	GROUP BY cohort_date, index
	ORDER BY cohort_date, index)


SELECT 
  cohort_date,
  SUM(CASE WHEN index=1 THEN customer_count ELSE 0 END) AS m1,
  SUM(CASE WHEN index=2 THEN customer_count ELSE 0 END) AS m2,
  SUM(CASE WHEN index=3 THEN customer_count ELSE 0 END) AS m3,
  SUM(CASE WHEN index=4 THEN customer_count ELSE 0 END) AS m4,
  SUM(CASE WHEN index=5 THEN customer_count ELSE 0 END) AS m5,
  SUM(CASE WHEN index=6 THEN customer_count ELSE 0 END) AS m6,
  SUM(CASE WHEN index=7 THEN customer_count ELSE 0 END) AS m7,
  SUM(CASE WHEN index=8 THEN customer_count ELSE 0 END) AS m8,
  SUM(CASE WHEN index=9 THEN customer_count ELSE 0 END) AS m9,
  SUM(CASE WHEN index=10 THEN customer_count ELSE 0 END) AS m10,
  SUM(CASE WHEN index=11 THEN customer_count ELSE 0 END) AS m11,
  SUM(CASE WHEN index=12 THEN customer_count ELSE 0 END) AS m12,
  SUM(CASE WHEN index=13 THEN customer_count ELSE 0 END) AS m13,
  SUM(CASE WHEN index=14 THEN customer_count ELSE 0 END) AS m14,
  SUM(CASE WHEN index=15 THEN customer_count ELSE 0 END) AS m15,
  SUM(CASE WHEN index=16 THEN customer_count ELSE 0 END) AS m16,
  SUM(CASE WHEN index=17 THEN customer_count ELSE 0 END) AS m17,
  SUM(CASE WHEN index=18 THEN customer_count ELSE 0 END) AS m18,
  SUM(CASE WHEN index=19 THEN customer_count ELSE 0 END) AS m19,
  SUM(CASE WHEN index=20 THEN customer_count ELSE 0 END) AS m20,
  SUM(CASE WHEN index=21 THEN customer_count ELSE 0 END) AS m21,
  SUM(CASE WHEN index=22 THEN customer_count ELSE 0 END) AS m22,
  SUM(CASE WHEN index=23 THEN customer_count ELSE 0 END) AS m23,
  SUM(CASE WHEN index=24 THEN customer_count ELSE 0 END) AS m24,
  SUM(CASE WHEN index=25 THEN customer_count ELSE 0 END) AS m25,
  SUM(CASE WHEN index=26 THEN customer_count ELSE 0 END) AS m26,
  SUM(CASE WHEN index=27 THEN customer_count ELSE 0 END) AS m27,
  SUM(CASE WHEN index=28 THEN customer_count ELSE 0 END) AS m28,
  SUM(CASE WHEN index=29 THEN customer_count ELSE 0 END) AS m29,
  SUM(CASE WHEN index=30 THEN customer_count ELSE 0 END) AS m30,
  SUM(CASE WHEN index=31 THEN customer_count ELSE 0 END) AS m31,
  SUM(CASE WHEN index=32 THEN customer_count ELSE 0 END) AS m32,
  SUM(CASE WHEN index=33 THEN customer_count ELSE 0 END) AS m33,
  SUM(CASE WHEN index=34 THEN customer_count ELSE 0 END) AS m34,
  SUM(CASE WHEN index=35 THEN customer_count ELSE 0 END) AS m35,
  SUM(CASE WHEN index=36 THEN customer_count ELSE 0 END) AS m36
  FROM public.vw_cohort
  GROUP BY cohort_date
  ORDER BY cohort_date
  -- Ai là khách hàng tốt nhất, phân tích dựa vào RFM 



