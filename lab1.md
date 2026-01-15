# Flink SQL Workshop

### Understanding Flink SQL & Query Execution

Powered by Faker connector https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html

With a faker table, data is not being generated continuously. Instead, when a statement is started that reads from the sample data, it is instantiated in this specific context and generates data only to be read by this specific statement. If multiple statements read from the same faker table at the same time, they get different data.

```
CREATE TABLE `transactions_faker` (
  `txn_id` VARCHAR(36) NOT NULL,
  `account_number` VARCHAR(255),
  `timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE,
  `amount` DECIMAL(10, 2),
  `currency` VARCHAR(5),
  `merchant` VARCHAR(255),
  `location` VARCHAR(255),
  `status` VARCHAR(255),
  `transaction_type` VARCHAR(50),
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECONDS)
DISTRIBUTED BY HASH(`txn_id`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'faker',
  'fields.account_number.expression' = 'ACC#{Number.numberBetween ''1000000'',''1000010''}',
  'fields.amount.expression' = '#{NUMBER.numberBetween ''10'',''1000''}',
  'fields.currency.expression' = '#{Options.option ''USD'',''EUR'',''INR'',''GBP'',''JPY''}',
  'fields.location.expression' = '#{Options.option ''New York'',''Los Angeles'',''Chicago'',''Charlotte '',''San Francisco'',''Indianapolis'',''Seattle'',''Denver'',''Washington'',''Boston'',''El Paso'',''Nashville'',''Detroit'',''Oklahoma City'',''Portland'',''Las Vegas'',''Memphis'',''Louisville'',''Baltimore''}',
  'fields.merchant.expression' = '#{Options.option ''Walmart Inc.'', ''Amazon.com Inc.'', ''CVS Health'', ''Costco Wholesale Corporation'', ''Schwarz Group'', ''McKesson Corporation'', ''McDonalds Corporation'', ''Starbucks Corporation'', ''Cencora'', ''The Home Depot Inc.'', ''Yum! Brands'', ''The Kroger Co.'', ''Aldi Group'', ''Walgreens Boots Alliance'', ''Cardinal Health'', ''Subway'', ''JD.com Inc.'', ''Target Corporation'', ''Ahold Delhaize'', ''Lowe Companies Inc.''}',
  'fields.transaction_type.expression' = '#{Options.option ''payment'',''payment'', ''payment'' ,''refund'', ''withdrawal''}',
  'fields.status.expression' = '#{Options.option ''Successful'',''Successful'', ''Failed'' }',
  'fields.txn_id.expression' = '#{IdNumber.valid}',
  'fields.timestamp.expression' = '#{date.past ''5'',''SECONDS''}',
  'rows-per-second' = '3')
```

Understand how table is created.

`SELECT * from transactions_faker`

`SHOW CREATE TABLE transactions_faker`

Check watermark value.

`DESCRIBE EXTENDED transactions_faker`

In case you need to recreate the table you can always drop it. 
`DROP TABLE transactions_faker`


```
CREATE TABLE `customers_faker` (
  `account_number` VARCHAR(2147483647) NOT NULL,
  `customer_name` VARCHAR(2147483647),
  `email` VARCHAR(2147483647),
  `phone_number` VARCHAR(2147483647),
  `date_of_birth` TIMESTAMP(3),
  `city` VARCHAR(2147483647),
  `created_at` TIMESTAMP(3) WITH LOCAL TIME ZONE
)
DISTRIBUTED INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'faker',
  'fields.account_number.expression' = 'ACC#{Number.numberBetween ''1000000'',''1000010''}',
  'fields.city.expression' = '#{Address.city}',
   'fields.customer_name.expression' = '#{Name.fullName}',
  'fields.date_of_birth.expression' = '#{date.birthday ''18'',''50''}',
  'fields.phone_number.expression' = '#{PhoneNumber.cellPhone}',
  'fields.email.expression' = '#{Internet.emailAddress}',
  'fields.created_at.expression' = '#{date.past ''3'',''SECONDS''}',
  'rows-per-second' = '1'
)
```
`SELECT * from customers_faker`

Verify the physical plan for the CTAS query.

```
EXPLAIN 
CREATE TABLE customers_pk (
  PRIMARY KEY(account_number) NOT ENFORCED,
  WATERMARK FOR `created_at` AS `created_at`  - INTERVAL '5' SECONDS
)
  WITH ('changelog.mode' = 'upsert')
AS SELECT * FROM `customers_faker`
```

Create a new table with primary key derived from the append table.
Set idle timeout for all partitions

```
SET 'sql.tables.scan.idle-timeout' = '1s';
CREATE TABLE customers_pk (
  PRIMARY KEY(account_number) NOT ENFORCED,
  WATERMARK FOR `created_at` AS `created_at`  - INTERVAL '5' SECONDS
)
  WITH ('changelog.mode' = 'upsert')
AS SELECT * FROM `customers_faker`
```

`SHOW CREATE TABLE customers_pk`

`DESCRIBE EXTENDED customers_pk`

Switch to changelog mode output. 

`SELECT * from customers_pk`

Add some metadata.

```
ALTER TABLE customers_pk ADD (
record_headers MAP<STRING, BYTES> METADATA FROM 'headers',
  record_leader_epoch INT METADATA FROM 'leader-epoch',
  record_offset BIGINT METADATA FROM 'offset',
  record_partition INT METADATA FROM 'partition',
  record_key BYTES METADATA FROM 'raw-key',
  record_value BYTES METADATA FROM 'raw-value',
  record_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  record_timestamp_type STRING METADATA FROM 'timestamp-type',
  record_topic STRING METADATA FROM 'topic'
);
```
`DESCRIBE EXTENDED customers_pk`

`SELECT * from customers_pk`

Change the changelog mode to Append

``ALTER TABLE `customers_pk` SET ('changelog.mode' = 'append');``

`SELECT * from customers_pk`

Change it back to Upsert

``ALTER TABLE `customers_pk` SET ('changelog.mode' = 'upsert');``

Change isolation level to read uncommitted data. 

``ALTER TABLE `customers_pk` SET ('kafka.consumer.isolation-level'='read-uncommitted')``

`SELECT * from customers_pk`


