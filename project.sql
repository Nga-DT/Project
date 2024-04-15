-- Nhập dữ liệu vào PostgreSQL
CREATE TABLE customer_shopping_data
  (invoice_no VARCHAR,
   customer_id VARCHAR,
   gender VARCHAR,
   age INT,
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
     ROW_NUMBER () OVER (PARTITION BY customer_id,gender,age,category,quantity,price,payment_method,invoice_date,shopping_mall ORDER BY invoice_no)AS stt, * 
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
  -- Số lượng đơn hàng và số lượng khách hàng mỗi tháng
  -- Giá trị đơn hàng trung bình (AOV) và số lượng khách hàng mỗi tháng
  -- Nhóm khách hàng theo độ tuổi
  -- Top 5 sản phẩm mỗi tháng
  -- Cohort analysis
  -- Doanh thu theo từng ProductLine, Year 
  -- Đâu là tháng có bán tốt nhất mỗi năm?
  -- Đâu là sản phẩm có doanh thu tốt nhất ở UK mỗi năm?
  -- Ai là khách hàng tốt nhất, phân tích dựa vào RFM 



