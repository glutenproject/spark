SELECT ss_quantity, ss_wholesale_cost, sr_fee
FROM (
	SELECT *
	FROM store_sales
	LIMIT 100000
) store_sales2
	JOIN item
	JOIN (
		SELECT *
		FROM store_returns
		LIMIT 100000
	) store_returns2
	ON store_sales2.ss_item_sk = item.i_item_sk
		AND store_returns2.sr_item_sk = item.i_item_sk
WHERE item.i_color = 'pale'
	AND item.i_current_price > 50
	AND item.i_size = 'medium'