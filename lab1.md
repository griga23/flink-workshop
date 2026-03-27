# Lab 1: Understanding Flink SQL & Query Execution

In this lab you will learn how to execute Flink SQL statements to create and alter tables, and execute queries.
You will also learn how to analyze the execution plan of a query before submitting it to Flink for execution.

### 0 - Working with table references

In the *SQL Workspace*, select the default Catalog and Database. This allows you to refer to tables just by table name.

You can always use fully-qualified references in the form `<catalog>.<database>.<table>`.

> Note that in this first lab, table and field names are all quoted using backticks (`). 
> Quoting is not strictly required if the identifier only contains lowercase alphanumeric characters and underscore.

### 1 - Generate fake data using the Faker connector

The [Faker connector](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html) allows you to generate semi-random fake data matching a specific schema.

With a faker table, data is not being generated continuously. Instead, when a statement is started that reads from the sample data, it is instantiated in this specific context and generates data only to be read by this specific statement. If multiple statements read from the same faker table at the same time, they get different data.


#### 1.1 - Transactions fake data generator

Let's create the Faker source table for Transactions.

Create the table:

```sql
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
  'fields.amount.expression' = '#{Number.numberBetween ''10'',''1000''}',
  'fields.currency.expression' = '#{Options.option ''USD'',''EUR'',''INR'',''GBP'',''JPY''}',
  'fields.location.expression' = '#{Options.option ''New York'',''Los Angeles'',''Chicago'',''Charlotte'',''San Francisco'',''Indianapolis'',''Seattle'',''Denver'',''Washington'',''Boston'',''El Paso'',''Nashville'',''Detroit'',''Oklahoma City'',''Portland'',''Las Vegas'',''Memphis'',''Louisville'',''Baltimore''}',
  'fields.merchant.expression' = '#{Options.option ''Walmart Inc.'', ''Amazon.com Inc.'', ''CVS Health'', ''Costco Wholesale Corporation'', ''Schwarz Group'', ''McKesson Corporation'', ''McDonalds Corporation'', ''Starbucks Corporation'', ''Cencora'', ''The Home Depot Inc.'', ''Yum! Brands'', ''The Kroger Co.'', ''Aldi Group'', ''Walgreens Boots Alliance'', ''Cardinal Health'', ''Subway'', ''JD.com Inc.'', ''Target Corporation'', ''Ahold Delhaize'', ''Lowe Companies Inc.''}',
  'fields.transaction_type.expression' = '#{Options.option ''payment'',''payment'', ''payment'' ,''refund'', ''withdrawal''}',
  'fields.status.expression' = '#{Options.option ''Successful'',''Successful'', ''Failed'' }',
  'fields.txn_id.expression' = '#{IdNumber.valid}',
  'fields.timestamp.expression' = '#{date.past ''5'',''SECONDS''}',
  'rows-per-second' = '3')
```

Verify that the table has been created:

```sql
SHOW CREATE TABLE `transactions_faker`
```

##### Describe the schema

```sql
DESCRIBE EXTENDED `transactions_faker`
```

In particular, check out the Watermark in the `timestamp` column.

##### Show the fake data

```sql
SELECT * FROM `transactions_faker`
```

This starts the data generation. The query keeps running and generating data until you hit `STOP`.


> ⚠️ In case you need to recreate the table, drop it first before recreating: `DROP TABLE transactions_faker`

#### 1.2 - Customers fake data generator

Similarly, let's create the Faker source table for Customers.

Create the table:

```sql
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

##### Verify data is generated

```sql
SELECT * FROM `customers_faker`
```

---

### 2 - Show the physical plan of a query

We now want to write a query and analyze the query plan.

In particular, we create a `CREATE TABLE AS SELECT` (CTAS) statement which
1. Creates a new table, and
2. Populates it with the result of a `SELECT` query

Before executing it, we want to analyze the query plan using `EXPLAIN`. 

Just prepend `EXPLAIN` to the statement to show the query plan (execute the following SQL):

```sql
EXPLAIN
CREATE TABLE `customers_pk` (
  PRIMARY KEY(`account_number`) NOT ENFORCED,
  WATERMARK FOR `created_at` AS `created_at` - INTERVAL '5' SECONDS
)
  WITH ('changelog.mode' = 'upsert')
AS SELECT * FROM `customers_faker`
```


As you can see, Flink shows the Physical Plan of the query, which comprises all the steps (or *Operators*) Flink will use to execute your query.

Observe how this statement will (1) read from the source `customers_faker` table, 
(2) perform some row-level transformations, to add the watermark in this case, 
and (3) sink to the `customers_pk` topic using the Primary Key (`account_number`) as message key.

For more details about reading the query plan, see [EXPLAIN Statement in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html) documentation page.

We will get back to query plans and operators later. 

Note that nothing really happens with your data when you execute `EXPLAIN`. No table or topic are created, no data is generated.

> ⚠️ If you accidentally ran the `CREATE TABLE ... AS SELECT ...`, stop the running statement and drop the table before proceeding
> using `DROP TABLE customers_pk`. 

---

### 3 - Create Customers table with Primary Key

We will now execute the *CTAS* (*Create Table As Select*) statement we explained in the previous section to create the table representing Customers, using `account_number` as PK.

Execute the following CTAS statement:

```sql
CREATE TABLE `customers_pk` (
  PRIMARY KEY(`account_number`) NOT ENFORCED,
  WATERMARK FOR `created_at` AS `created_at` - INTERVAL '5' SECONDS
)
  WITH ('changelog.mode' = 'upsert')
AS SELECT * FROM `customers_faker`
```

##### Verify details of table

You can view details of the table you just created by showing the CREATE TABLE statement.

> ⚠️ Execute these statements in a different panel from the one where you executed `CREATE TABLE customers_pk ...` to keep the previous job running. 

```sql
SHOW CREATE TABLE `customers_pk`
```

You can also describe the table:

```sql
DESCRIBE EXTENDED `customers_pk`
```

##### Show the Customers changelog

Execute the following query:

```sql
SELECT * FROM `customers_pk`
```

Switch to the *Changelog view* to show the `+I`, `+U`, and `-U` [changelog records](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html#changelog-entries) sent to the topic every time the record of a given primary key is inserted or updated. Note that every update has two separate records, before (`-U`) and after (`+U`).

Stop this query before proceeding.

> **CTAS statements, automatic topic and schema creation**
>
> CTAS ("Create Table As Select") statements are a shortcut for `CREATE TABLE ...` + `INSERT INTO ... SELECT ...`.
> 
> Flink infers the resulting schema and creates the topic for the destination table. 
> 
> It also starts a continuously running statement which populates the destination table and writes data into the topic. 
> You can see the statement in *Environments > [select environment] > Flink > Flink statements*.
>
> Note that, when we create this new table, a topic called `customers_pk` is also created in the cluster. 
> Conversely, when we created the two *faker* tables, no topic and no schema were created.
> 
> You can find the new topic in *Environments > [select environment] > Clusters > [select cluster] > Topics*


---

### 4 - Alter a table

#### Add metadata columns

Let's alter our Customers table to add some new columns from the [metadata](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#flink-sql-metadata-columns) automatically available.

Alter the table to add columns derived from metadata:

```sql
ALTER TABLE `customers_pk` ADD (
  `record_headers` MAP<STRING, BYTES> METADATA FROM 'headers',
  `record_leader_epoch` INT METADATA FROM 'leader-epoch',
  `record_offset` BIGINT METADATA FROM 'offset',
  `record_partition` INT METADATA FROM 'partition',
  `record_key` BYTES METADATA FROM 'raw-key',
  `record_value` BYTES METADATA FROM 'raw-value',
  `record_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  `record_timestamp_type` STRING METADATA FROM 'timestamp-type',
  `record_topic` STRING METADATA FROM 'topic'
);
```

> ℹ  Note that you can only [alter a table](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html#) 
> to add metadata and computed columns, or to alter the watermark and properties. You cannot modify the schema.

##### Describe the table to show the column we added

```sql
DESCRIBE EXTENDED `customers_pk`
```

##### Show the generated data


```sql
SELECT * FROM `customers_pk`
```

Observe the query output. In particular the `record_timestamp` column.

As you can see, the results are delayed up to 60 seconds.
This is because the default *Exactly-Once* delivery guarantee leverages Kafka transactions and Flink checkpoints, which commit
data written to the topic roughly every minute.

For more details about delivery guarantees, see [Delivery Guarantees and Latency in Confluent Cloud for Apache Flink](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html).

We can modify the delivery guarantees by changing the isolation level on the table, to allow reading uncommitted data.

#### Alter table property: change isolation level

Isolation level is controlled by the `kafka.consumer.isolation-level` table property. 
Let's alter the table again to change it to `read-uncommitted`.

```sql
ALTER TABLE `customers_pk` SET ('kafka.consumer.isolation-level'='read-uncommitted')
```

##### Show the data again

Show the generated data, again (stop the previous `SELECT...` and re-execute):

```sql
SELECT * FROM `customers_pk`
```

Observe the new query output, and in particular the `record_timestamp` column.

You can see now that data is refreshed more frequently and the delay is substantially lower than 60 seconds.

> ℹ  You can still see a delay of a few seconds. This is due to the refresh rate of the UI.
> Records are actually read from the topic with negligible delay, without waiting for the transaction commit.

---
---

Next: [Lab 2: Joins](lab2.md)
