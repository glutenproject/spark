SELECT sum(ss_wholesale_cost) / 7.0 AS avg_yearly
FROM store_sales, item
WHERE i_item_sk = ss_item_sk
	AND i_color = 'pale'
	AND i_current_price > 50
	AND i_size = 'medium'
	AND ss_quantity < (
		SELECT 0.2 * avg(sr_return_quantity)
		FROM store_returns
		WHERE sr_item_sk = i_item_sk
	)