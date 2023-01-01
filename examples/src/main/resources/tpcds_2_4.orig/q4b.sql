SELECT ss_quantity, ss_wholesale_cost, sr_fee, sr_return_ship_cost
FROM store_sales
    JOIN item
    JOIN store_returns
    ON store_sales.ss_item_sk = item.i_item_sk
        AND store_returns.sr_item_sk = item.i_item_sk
WHERE item.i_color = 'pale' AND item.i_current_price > 50 AND item.i_size = 'medium'