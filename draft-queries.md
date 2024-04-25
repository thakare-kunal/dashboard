## Draft Quries

#### Shipment TAT
```SELECT s.id, datediff( MAX(ssls.created_at), min(ssls.created_at) ) as a FROM `shipments` as s left join shipment_state_logs as ssls on ssls.shipment_id = s.id GROUP by s.id ORDER BY `a` ASC```
