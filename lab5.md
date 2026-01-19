
# Lab 5

### Statement Operations

Use demo data from Lab 1

#### Error Handling
Custom error handling for deserialization errors
https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html#custom-error-handling


Insert valid data
```
INSERT INTO customers_pk (account_number, customer_name, city)
VALUES ('ACC1000111', 'Bob Smith', 'Munich');
```

Try to insert invalid data
```
INSERT INTO customers_pk (account_number, customer_name, date_of_birth)
VALUES ('ACC1000112', 'Invalid Customer Date Format', '1.1.2015');
```

Try to insert NULL  for key
```
INSERT INTO customers_pk (account_number, customer_name, city)
VALUES (CAST(NULL AS STRING), 'Ghost User', 'London');
```

Try to insert incorrect column count
```
INSERT INTO customers_pk 
SELECT 
    'ACC999' as account_number, 
    'Broken Entry' as customer_name; 
```

Create a new table and copy some correct messages from faker. Stop it after 1 minute.
```
CREATE TABLE customers_poisoned AS
SELECT *
FROM `customers_faker`;
```

Enable error handling for the source table
```
ALTER TABLE `customers_poisoned` SET (
  'error-handling.mode' = 'log',
  'error-handling.log.target' = 'dlq_topic'
) 
```
Check if enabled
```
SHOW CREATE TABLE `customers_poisoned`
```

Start insert from the poisoned topic
```
INSERT INTO `customers_pk` SELECT * FROM `customers_poisoned`
```

Manually insert couple incorrect message to the Kafka topic `customers_poisoned` (produce without schema)
```
{"id": 1, "payload": "poisoned pill"}
```

Check DLQ log
```
SELECT * FROM `dlq_topic`
```

#### Carry Over Offset
https://docs.confluent.io/cloud/current/flink/operate-and-deploy/carry-over-offsets.html

set 'client.statement-name' = 'initial-statement-6';

```
CREATE TABLE carryover
  WITH ('kafka.consumer.isolation-level'='read-uncommitted')
  AS
SELECT *,`offset` FROM stocks
```
