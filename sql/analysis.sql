SELECT
    us_state,
    max(amount) as max_transaction_amount,
    argMax(cat_id, amount) as category_of_max_transaction
FROM default.transactions
GROUP BY us_state
ORDER BY max_transaction_amount DESC
FORMAT CSVWithNames;