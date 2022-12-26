select
	sum(ss_wholesale_cost) / 7.0 as avg_yearly
from
	store_sales,
	item
where
	i_item_sk = ss_item_sk
	and i_color = 'pale'
	and ss_quantity < (
		select
			0.2 * avg(sr_return_quantity)
		from
			store_returns
		where
			sr_item_sk = i_item_sk
	)