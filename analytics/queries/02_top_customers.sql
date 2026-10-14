SELECT customer_id, total_spent, total_orders
FROM ecom_lakehouse.gold_customer_summary
ORDER BY total_spent DESC
LIMIT 10
