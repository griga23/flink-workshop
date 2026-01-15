# Flink SQL Workshop

### Create initial tables

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
`SELECT * from transactions_faker`

`SHOW CREATE TABLE transactions_faker`

`DESCRIBE EXTENDED transactions_faker`

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

```
EXPLAIN 
CREATE TABLE customers_pk (
  PRIMARY KEY(account_number) NOT ENFORCED,
  WATERMARK FOR `created_at` AS `created_at`  - INTERVAL '5' SECONDS
)
  WITH ('changelog.mode' = 'upsert')
AS SELECT * FROM `customers_faker`
```


```
CREATE TABLE customers_pk (
  PRIMARY KEY(account_number) NOT ENFORCED,
  WATERMARK FOR `created_at` AS `created_at`  - INTERVAL '5' SECONDS
)
  WITH ('changelog.mode' = 'upsert')
AS SELECT * FROM `customers_faker`
```

`SHOW CREATE TABLE customers_pk`

`DESCRIBE EXTENDED customers_pk`

`SELECT * from customers_pk`
