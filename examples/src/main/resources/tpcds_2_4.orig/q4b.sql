SELECT ss_quantity, ss_wholesale_cost, sr_fee
FROM (
	SELECT ss_quantity, ss_wholesale_cost, ss_item_sk
	FROM store_sales
	WHERE ss_wholesale_cost BETWEEN 0 AND 50
) store_sales2
	JOIN item
	JOIN (
		SELECT sr_fee, sr_item_sk
		FROM store_returns
	) store_returns2
	ON store_sales2.ss_item_sk = item.i_item_sk
		AND store_returns2.sr_item_sk = item.i_item_sk
WHERE item.i_color = 'pale'
	AND item.i_current_price > 50
	AND item.i_size = 'medium'