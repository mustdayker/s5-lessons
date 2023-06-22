SELECT DISTINCT 
	json_array_elements(
		(event_value::JSON->>'product_payments')::JSON
	)::JSON->>'product_name' AS product_name
FROM outbox;