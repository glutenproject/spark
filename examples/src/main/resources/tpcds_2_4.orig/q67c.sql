SELECT *
FROM
  (SELECT
    ss_sold_date_sk,
    ss_sold_time_sk,
    ss_item_sk,
    ss_customer_sk,
    rank()
    OVER (PARTITION BY ss_ticket_number
      ORDER BY ss_wholesale_cost DESC) rk
  FROM
    store_sales)
WHERE rk <= 100
