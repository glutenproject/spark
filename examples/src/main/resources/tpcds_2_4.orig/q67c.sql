SELECT *
FROM (
	SELECT ss_ticket_number, sumsales, rank() OVER (PARTITION BY ss_ticket_number ORDER BY sumsales DESC) AS rk
	FROM (
		SELECT ss_ticket_number
			, sum(coalesce(ss_sales_price * ss_quantity, 0)) AS sumsales
		FROM store_sales
		GROUP BY ss_ticket_number
	)
)
WHERE rk <= 100
