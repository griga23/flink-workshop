# Flink SQL Workshop: Advanced Stream Processing with Confluent Cloud

Welcome to the hands-on portion of our Flink SQL workshop. This repository contains all the lab exercises and SQL scripts required to master streaming analytics.

---

## 🛠 Prerequisites
* Access to a Confluent Cloud Flink SQL environment.
* Basic understanding of SQL syntax.
* Data generator (Faker) or Kafka topics configured as per the workshop setup guide.

---

## 📚 Workshop Labs

Navigate through the following labs to complete the practical blocks:

| Lab # | Topic | Description |
| :--- | :--- | :--- |
| **[Lab 1: Foundations](lab1.md)** | **Understanding Flink SQL & Query Execution** | Interpreting `EXPLAIN` plans, mapping physical columns, and capturing Kafka Metadata. |
| **[Lab 2: State Operators](lab2.md)** | **Highly State-Intensive Operators** | Implementing Temporal Table Joins and window strategies while managing state impact. |
| **[Lab 3: Hints & Views ](lab3.md)** | **SQL Hints, Views & Schema Evolution** | Using `HINTS` for statement improvement, creating `VIEW`s, and using Statement Sets. |
| **[Lab 4: Changelogs](lab4.md)** | **Changelog Modes & Table Semantics** | Observing **Retractions**, using the `ProcessTableFunction`, and handling Primary Keys. |
| **[Lab 5: Statement Operations](lab5.md)** | **Statement Operations & Bottlenecks** | Error Handling, Statement Updates, and Identifying data skew. |
---

## 🚀 Running the Labs
1. **Open your environment**: Launch your Console Flink SQL Client or Confluent Cloud SQL Workspace.
2. **Execute DDL**: Copy the `CREATE TABLE` statements from the relevant lab file to register your sources.
3. **Run Queries**: Execute the practical exercises.
4. **Verify**: Cehck your results
