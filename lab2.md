# Lab 2

### Highly State-Intensive Operators

Use demo data from Lab 1

#### Temporal Join
Join the transactions_faker (the heart-beat stream) with customers_pk (the versioned dimension table).

```
SELECT 
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_pk FOR SYSTEM_TIME AS OF t.`timestamp` AS c
    ON t.account_number = c.account_number;
```
    
#### Interval Join
Interval Join looks for occurrences where a transaction and a customer creation event happened within a specific time window of each other.

```
SELECT 
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number
WHERE t.`timestamp` BETWEEN c.created_at AND c.created_at + INTERVAL '10' SECOND;
```

#### Regular Join with TTL

Without TTL
```
SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```

With TTL
```
SET 'sql.state-ttl' = '2d';
SELECT
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker AS t
JOIN customers_faker AS c
    ON t.account_number = c.account_number;
```
Use `EXPLAIN` with the above joins to understand the different operators used. 

#### MongoDB Lookup Join

Define connectivity to the MongoDB (requires username/password)

```
CREATE CONNECTION mongodb_connection
  WITH (
    'type' = 'mongodb',
    'endpoint' = 'mongodb+srv://janscluster.emg9svf.mongodb.net/',
    'username' = '<atlas_username>',
    'password' = '<atlas_password>'
  );
```

Create table with MongoDB connectivity with the lookup schema
```
CREATE TABLE mongodb_movies_key_search (
  title STRING,
  genres ARRAY<STRING>,
  rated STRING,
  plot STRING,
  `year` INT
) WITH (
  'connector' = 'mongodb',
  'mongodb.connection' = 'mongodb_connection',
  'mongodb.database' = 'sample_mflix',
  'mongodb.collection' = 'movies',
  'mongodb.index' = 'default'
);
```

Select movies from the year that customer was born

```
SELECT 
  customer_name, 
  date_of_birth, 
  search_results 
FROM customers_faker,
  LATERAL TABLE(
     KEY_SEARCH_AGG(
       `mongodb_movies_key_search`, 
        descriptor(`year`), 
        CAST(YEAR(date_of_birth) AS INT))
  )
```
https://docs.confluent.io/cloud/current/ai/external-tables/key-search.html#key-search-with-mongodb

#### GROUP BY

Select customers with sum of transactions larger than 500
```
SELECT 
  account_number,
  transaction_type,
  SUM(amount) 
FROM `transactions_faker` 
WHERE transaction_type = 'withdrawal' 
GROUP BY account_number,transaction_type
HAVING SUM(amount) > 500
```

#### OVER()
Select customers with sum of transactions larger than 500 and show row updates.
```
SELECT
    `account_number`,
    transaction_type,
    amount,
    SUM(amount) OVER w as total_value,
    CASE WHEN SUM(amount) OVER  w  > '500' THEN 'YES' ELSE 'NO' END AS FLAG
FROM `transactions_faker`
WHERE transaction_type = 'withdrawal'
WINDOW w AS (
 PARTITION BY account_number,transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
```

#### TUMBLE WINDOW GROUP BY
Count failed transaction in 1 minute intervals for each merchant. 
Emit results after 1 minute
```
SELECT 
  window_start,
  window_end,
  merchant,
  COUNT(*) AS total_tx_failed
FROM
TUMBLE(
  TABLE `transactions_faker`, 
  DESCRIPTOR (`timestamp`), 
  INTERVAL '1' MINUTE
  )
WHERE transaction_type ='payment' AND status = 'Failed'
GROUP BY 
  window_start, 
  window_end,
  merchant
```

#### SESSION WINDOW GROUP BY
Count failed transactions in merchant sessions. Session Gap is 2 seconds
```
SELECT
  window_start,
  window_end,
  merchant,
  COUNT(*) AS total_tx_failed
FROM
  SESSION(
    TABLE `transactions_faker`,
    DESCRIPTOR(`timestamp`),
    INTERVAL '2' SECONDS
  )
WHERE transaction_type = 'payment' AND status = 'Failed'
GROUP BY
  window_start,
  window_end,
  merchant;
```

#### OVER WINDOW 
Count failed transaction in 1 minute intervals for each merchant.
Emit results immediately 

```
SELECT 
  `timestamp`,
  merchant,
  COUNT(*) OVER w AS total_tx_failed_last_minute
FROM `transactions_faker`
WHERE transaction_type = 'payment' AND status = 'Failed'
WINDOW w AS (
  PARTITION BY merchant 
  ORDER BY `timestamp` 
  RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
);
```

#### COMBINE GROUP BY and OVER WINDOW
Phase 1 (Group By): Calculate the count of failed transactions for every merchant per minute.
Phase 2 (Over): Flink looks at the window results and compares the current count to the previous count using LAG.

```
SELECT 
  window_start,
  window_end,
  merchant,
  transaction_type,
  COUNT(*) as total_failed,
  LAG(COUNT(*),1) OVER w as prev_total_failed,
  COUNT(*) - LAG(COUNT(*),1) OVER w as delta
FROM
TUMBLE(
  TABLE `transactions_faker`, 
  DESCRIPTOR (`timestamp`), 
  INTERVAL '1' MINUTE
  )
WHERE transaction_type ='payment' AND status = 'Successful'
GROUP BY 
  window_start, 
  window_end,
  window_time,
  merchant, 
  transaction_type
WINDOW w as (
  PARTITION BY merchant,transaction_type 
  ORDER BY window_time
  ) 
```
