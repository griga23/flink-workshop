
# Lab 4

### Changelog Modes & Table Semantics

Use demo data from Lab 1

Change the changelog mode to Append

``ALTER TABLE `customers_pk` SET ('changelog.mode' = 'append');``

`SELECT * from customers_pk`

Change it back to Upsert

``ALTER TABLE `customers_pk` SET ('changelog.mode' = 'upsert');``

`SELECT * from customers_pk`

Bad query example (Shuffle, ChangelogNormalizer and GroupAggregate)
Calculate the total spend of every city based on incoming transactions
```
SELECT 
    c.city,
    SUM(t.amount) AS total_spent,
    COUNT(t.txn_id) AS transaction_count
FROM transactions_faker AS t
JOIN customers_pk AS c 
    ON t.account_number = c.account_number
GROUP BY c.city;
```

Key switching nightmare (multiple StreamExchange)
Look for customers who have spent a significant amount of money and aggregate them by location
```
SELECT 
    city,
    COUNT(*) as high_spender_count
FROM (
    SELECT 
        c.email,
        c.city,
        SUM(t.amount) as total_spent
    FROM transactions_faker t
    JOIN customers_pk c ON t.account_number = c.account_number 
    GROUP BY c.email, c.city
)
WHERE total_spent > 1000
GROUP BY city;
```

Primary key differs from upsert key
https://docs.confluent.io/cloud/current/flink/how-to-guides/resolve-common-query-problems.html

Join transactions with customer info.
Full outer join 
```
SELECT 
    COALESCE(t.account_number, c.account_number) as account_id,
    t.txn_id,
    t.amount,
    c.customer_name,
    c.city
FROM transactions_faker t
FULL OUTER JOIN customers_faker c
    ON t.account_number = c.account_number;
```

Over Window
```
WITH combined_data AS (
    SELECT
        account_number,
        txn_id,
        amount,
        CAST(NULL AS STRING) as customer_name,
        CAST(NULL AS STRING) as city,
        `timestamp` as event_time
    FROM transactions_faker
    UNION ALL
    SELECT
        account_number,
        CAST(NULL AS STRING) as txn_id,
        CAST(NULL AS DOUBLE) as amount,
        customer_name, 
        city,
        created_at as event_time
    FROM customers_faker
)
SELECT
    account_number,
    LAST_VALUE(txn_id) OVER w AS last_txn_id,
    LAST_VALUE(amount) OVER w AS last_amount,
    LAST_VALUE(customer_name) OVER w AS last_customer_name,
    LAST_VALUE(city) OVER w AS city
FROM combined_data
WINDOW w AS (
    PARTITION BY account_number 
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
```
