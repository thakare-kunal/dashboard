## Queries
### Get average ticket size with tax and without tax
- ```select AVG(total), avg(t1) from ( SELECT sale_order_id, max(final_invoice_total_with_tax) as total, sum(total_unit_price_without_tax) as t1 from regular_orders where order_state <> "CANCELLED" GROUP BY sale_order_id ) as a;```

### Average Discount
- ```select AVG(sale_order_item_discount_percentage) from regular_orders where is_dc = 1 and final_invoice_total_with_tax > 0 and order_state <> "CANCELLED";```

### CC percentage
- ```select cc.cc_total / b.total * 100 from ( SELECT sum(total_ii_quanity) as cc_total FROM `regular_orders` where invoice_prefix = "CC" and order_state <> "CANCELLED" ) as cc, ( SELECT SUM(total_ii_quanity) as total from regular_orders where order_state <> "CANCELLED") as b;```
