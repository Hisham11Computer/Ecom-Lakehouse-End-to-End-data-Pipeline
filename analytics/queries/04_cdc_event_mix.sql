SELECT op, COUNT(*) AS events
FROM ecom_lakehouse.bronze_orders
GROUP BY op
ORDER BY events DESC
