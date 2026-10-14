SELECT product_name,
       COUNT(*) AS orders,
       ROUND(SUM(total_amount), 2) AS revenue
FROM ecom_lakehouse.silver_orders
GROUP BY product_name
ORDER BY revenue DESC
