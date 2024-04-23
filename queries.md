## Queries
- Get average ticket size with tax and without tax
```select AVG(total), avg(t1) from ( SELECT sale_order_id, max(final_invoice_total_with_tax) as total, sum(total_unit_price_without_tax) as t1 from regular_orders where order_state <> "CANCELLED" GROUP BY sale_order_id ) as a;```
