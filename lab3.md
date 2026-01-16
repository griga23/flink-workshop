# Lab 3

### SQL Hints, VIEWS & ALTER TABLE

Use demo data from Lab 1

#### SET

Multiple SETs
```
SET 'sql.state-ttl' = '2d';
SET 'sql.tables.scan.startup.mode' = 'latest-offset';
SET 'sql.local-time-zone' = 'CET';
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

SET available options
https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#available-set-options

This just doesn't do anything;) 
```
SET 'sql.dry-run' = 'true';
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

Testing Snapshot queries
https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html#flink-sql-snapshot-queries

Check Operators used.
```
SET 'sql.snapshot.mode' = 'now';
EXPLAIN
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

Query doesn't give intermediate results and is in completed status. 
```
SET 'sql.snapshot.mode' = 'now';
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

Disable Watermark alignment across partitions
https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html#flink-sql-watermarks-watermark-alignment

Eliminate any watermark effects. No late data allowed. 
```
SET 'sql.tables.scan.watermark-alignment.max-allowed-drift' = '0';
SET 'sql.tables.scan.idle-timeout' = '500ms';
SET 'sql.state-ttl' = '5 seconds';
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

#### HINTS

All the HINTS https://docs.confluent.io/cloud/current/flink/reference/statements/hints.html

Different offset and TTL strategies for topics.
```
SELECT /*+ STATE_TTL('t'='10s', 'c'='1h') */
    t.txn_id,
    t.amount,
    t.`timestamp` AS txn_time,
    c.customer_name,
    c.created_at AS signup_time
FROM transactions_faker /*+ OPTIONS('scan.startup.mode'='latest-offset') */  AS t
JOIN customers_faker /*+ OPTIONS('scan.startup.mode'='earliest-offset') */  AS c 
    ON t.account_number = c.account_number;
```

#### ALTER TABLE
https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html

Change table property
``ALTER TABLE `transactions_faker` SET ('rows-per-second' = '20');``

Modify table metadata

``ALTER TABLE `transactions_faker` 
MODIFY WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECONDS;``

Add computed column
``ALTER TABLE `transactions_faker` 
ADD my_time AS  CONVERT_TZ(CAST(`timestamp` AS STRING), 'UTC', 'Europe/Berlin') ;``

`DESCRIBE EXTENDED transactions_faker`

`SELECT * FROM transactions_faker`

Change changelog modes
``ALTER TABLE `customers_pk` SET ('changelog.mode' = 'append');``

`SELECT * FROM customers_pk`

``ALTER TABLE `customers_pk` SET ('changelog.mode' = 'upsert');``

Changes the scan range on the table
```
ALTER TABLE customers_pk SET (
  'scan.startup.mode' = 'latest-offsets',
);
```

``SHOW CREATE TABLE `customers_pk`;``

#### CTE
Calculate the total withdrawal amount for each account number. Flag & Filter those transactions which breach the $10000 threshold and also Flag & Filter  those transactions if the total amount spent on a particular currency is more than $6000 in the last 2 hours.

```
WITH flagged_withdrawal AS (
SELECT `account_number`,`currency`,transaction_type,amount,merchant, location,
    SUM(amount) OVER  w as total_value,
     CASE WHEN SUM(amount) OVER  w  > '1000' THEN 'YES'
     ELSE 'NO' END AS FLAG
    FROM `transactions_faker`
    WHERE transaction_type = 'withdrawal'
WINDOW w AS (
 PARTITION BY account_number,transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW)
)
SELECT * FROM flagged_withdrawal WHERE FLAG = 'YES'
```

You can’t include two different OVER() window within the same subquery/query. But creating separate queries to compute this new metric seems futile and redundant. That’s why we leverage an additional CTE to compute metrics over two separate OVER windows.

```
WITH flagged_withdrawal_tx AS (
SELECT `txn_id`,`account_number`,
    SUM(amount) OVER  w as total_value,
     CASE WHEN SUM(amount) OVER  w  > '500' THEN 'YES'
     ELSE 'NO' END AS WITHDRAW_FLAG
    FROM `transactions_faker`
    WHERE transaction_type = 'withdrawal' and status = 'Successful'
WINDOW w AS (
 PARTITION BY account_number,transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
),
flagged_currency_tx AS (
SELECT `txn_id`,`account_number`,
    SUM(amount) OVER  w as total_value,
     CASE WHEN SUM(amount) OVER  w  > '300' THEN 'YES'
     ELSE 'NO' END AS FX_FLAG
    FROM `transactions_faker`
    WHERE transaction_type <> 'refund' and status = 'Successful'
WINDOW w AS (
 PARTITION BY account_number,currency
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
  
)
SELECT 
    A.*,
    B.total_value AS withdrawal_total,
    COALESCE(B.WITHDRAW_FLAG,'NO') as WITHDRAW_FLAG ,
    C.total_value AS forex_total,C.FX_FLAG 
FROM `transactions_faker` A 
    LEFT JOIN flagged_withdrawal_tx B
ON A.txn_id = B.txn_id AND A.account_number = B.account_number
    LEFT JOIN flagged_currency_tx C 
ON A.txn_id = C.txn_id AND A.account_number = C.account_number
WHERE WITHDRAW_FLAG = 'YES' OR FX_FLAG = 'YES'

```

#### VIEWS
https://docs.confluent.io/cloud/current/flink/reference/statements/create-view.html

Reuse the previous query to define a new View.
```
CREATE VIEW flagged_trx_view AS

WITH flagged_withdrawal_tx AS (
SELECT `txn_id`,`account_number`,
    SUM(amount) OVER  w as total_value,
     CASE WHEN SUM(amount) OVER  w  > '500' THEN 'YES'
     ELSE 'NO' END AS WITHDRAW_FLAG
    FROM `transactions_faker`
    WHERE transaction_type = 'withdrawal' and status = 'Successful'
WINDOW w AS (
 PARTITION BY account_number,transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
),
flagged_currency_tx AS (
SELECT `txn_id`,`account_number`,
    SUM(amount) OVER  w as total_value,
     CASE WHEN SUM(amount) OVER  w  > '300' THEN 'YES'
     ELSE 'NO' END AS FX_FLAG
    FROM `transactions_faker`
    WHERE transaction_type <> 'refund' and status = 'Successful'
WINDOW w AS (
 PARTITION BY account_number,currency
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
  
)
SELECT 
    A.*,
    B.total_value AS withdrawal_total,
    COALESCE(B.WITHDRAW_FLAG,'NO') as WITHDRAW_FLAG ,
    C.total_value AS forex_total,C.FX_FLAG 
FROM `transactions_faker` A 
    LEFT JOIN flagged_withdrawal_tx B
ON A.txn_id = B.txn_id AND A.account_number = B.account_number
    LEFT JOIN flagged_currency_tx C 
ON A.txn_id = C.txn_id AND A.account_number = C.account_number
WHERE WITHDRAW_FLAG = 'YES' OR FX_FLAG = 'YES'
```

`SHOW CREATE VIEW flagged_trx_view`

`DESCRIBE  flagged_trx_view`

Use the View in a Select statement
```
SELECT 
  txn_id,
  account_number,
  CONCAT('Dear user ', CAST(account_number AS STRING),',
  your transaction of ',CAST(amount AS STRING),
  ' cannot be processed due to lack of ',CAST(currency AS STRING),' balance avilable on your card.') as message
FROM `flagged_trx_view` 
  WHERE `WITHDRAW_FLAG` = 'YES';
```

