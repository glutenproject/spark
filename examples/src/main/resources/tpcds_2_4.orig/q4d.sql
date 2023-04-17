SELECT *
FROM (
	SELECT *
	FROM store_sales
		JOIN store_returns ON ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
	WHERE sr_return_amt > 10000
) a
	JOIN (
		SELECT *
		FROM store_sales
			JOIN customer ON ss_customer_sk = c_customer_sk
		WHERE c_preferred_cust_flag = 'Y'
	) b
	ON a.sr_customer_sk = b.c_customer_sk
