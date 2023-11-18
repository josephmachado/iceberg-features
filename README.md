- [What is an Open Table Format (OTF) and when to use one? with Apache Iceberg](#what-is-an-open-table-format--otf--and-when-to-use-one--with-apache-iceberg)
- [Setup](#setup)
  * [Prerequisites](#prerequisites)
  * [Clean up](#clean-up)
  * [Docker spin up](#docker-spin-up)
  * [Create schema and tables](#create-schema-and-tables)
- [Apache Iceberg features](#apache-iceberg-features)
  * [Schema and Partition evolution](#schema-and-partition-evolution)
  * [Time Travel](#time-travel)
  * [Tagging](#tagging)
  * [Branching](#branching)
  * [Read from another system](#read-from-another-system)


# What is an Open Table Format (OTF) and when to use one? with Apache Iceberg

This repo is code for the blog: **[What is an Open Table Format (OTF) & why to use one, with Apache Iceberg](https://www.startdataengineering.com/post/what_why_table_format/)**

# Setup

## Prerequisites

Please install the following to follow along

1. [Docker](https://docs.docker.com/engine/install/)
2. [DuckDB](https://duckdb.org/docs/installation/)

**Note**: All the commands shown below are run via a terminal, If you are using Windows, use [WSL](https://ubuntu.com/tutorials/install-ubuntu-on-wsl2-on-windows-10#1-overview) to set up Ubuntu and run the following commands via that terminal.

## Clean up

Please use this command to clean up data from previous runs(if any).

```bash
rm -rf ./data
docker compose down
```
## Docker spin up

```bash
docker compose up --build -d
```

## Create schema and tables

First we need to sh into the docker container and start a spark shell, please use the commands shown below:

```bash
docker exec -ti local-spark bash
# You will be now in the spark container

# Start spark sql with iceberg and local warehouse
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/iceberg-warehouse \
    --conf spark.hadoop.hive.cli.print.header=true
```

**Docker down** Run this command to spin down the containers

```bash
docker compose down
```

In the spark sql shell, run the following commands to create a schema and table:

```sql
CREATE SCHEMA warehouse;
-- Use Apache Iceberg table format
DROP TABLE IF EXISTS local.warehouse.orders;
CREATE TABLE local.warehouse.orders (
    order_id string,
    cust_id INT,
    order_status string,
    order_date timestamp
) USING iceberg
PARTITIONED BY (date(order_date));

INSERT INTO local.warehouse.orders VALUES
('e481f51cbdc54678b7cc49136f2d6af7',69,'delivered',CAST('2023-11-01 09:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',87,'delivered',CAST('2023-11-01 10:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',125,'delivered',CAST('2023-11-01 11:56:33' AS TIMESTAMP));

INSERT INTO local.warehouse.orders VALUES
('53cdb2fc8bc7dce0b6741e2150273451',17,'delivered',CAST('2023-11-02 11:56:33' AS TIMESTAMP)),
('53cdb2fc8bc7dce0b6741e2150273451',19,'on_route',CAST('2023-11-02 12:56:33' AS TIMESTAMP));

INSERT INTO local.warehouse.orders VALUES
('47770eb9100c2d0c44946d9cf07ec65d',26,'on_route',CAST('2023-11-03 12:56:33' AS TIMESTAMP)),
('47770eb9100c2d0c44946d9cf07ec65d',99,'lost',CAST('2023-11-03 13:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',35,'delivered',CAST('2023-11-04 09:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',5,'lost',CAST('2023-11-04 10:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',105,'lost',CAST('2023-11-04 11:56:33' AS TIMESTAMP));

INSERT INTO local.warehouse.orders VALUES
('ad21c59c0840e6cb83a9ceb5573f8159',23,'delivered',CAST('2023-11-05 04:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',12,'on_route',CAST('2023-11-05 08:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',19,'delivered',CAST('2023-11-05 10:56:33' AS TIMESTAMP));

INSERT INTO local.warehouse.orders VALUES
('a4591c265e18cb1dcee52889e2d8acc3',82,'lost',CAST('2023-11-06 10:45:33' AS TIMESTAMP)),
('a4591c265e18cb1dcee52889e2d8acc3',1234,'on_route',CAST('2023-11-06 12:45:33' AS TIMESTAMP));
```

# Apache Iceberg features

## Schema and Partition evolution

```bash
-- schema evolution
ALTER TABLE local.warehouse.orders ALTER COLUMN cust_id TYPE bigint;
ALTER TABLE local.warehouse.orders DROP COLUMN order_status;

-- parititon evolution
ALTER TABLE local.warehouse.orders ADD PARTITION FIELD cust_id;
INSERT INTO local.warehouse.orders VALUES 
('e481f51cbdc54678b7cc49136f2d6af7',69,CAST('2023-11-14 09:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',87,CAST('2023-11-14 10:56:33' AS TIMESTAMP));

-- check snapshots
select committed_at, snapshot_id, manifest_list from local.warehouse.orders.snapshots;
-- check schema change and partition change history
select * from local.warehouse.orders.manifests;
```

## Time Travel

```sql
-- get the time of the first data snapshot
select min(committed_at) as min_committed_at from local.warehouse.orders.snapshots;
-- e.g. 2023-11-17 11:05:34.307

INSERT INTO local.warehouse.orders VALUES 
('e481f51cbdc54678b7cc49136f2d6af7',69,CAST('2023-11-20 09:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',87,CAST('2023-11-20 10:56:33' AS TIMESTAMP));

-- Query data as of that time or after
SELECT * FROM local.warehouse.orders TIMESTAMP AS OF '2023-11-17 11:06:00.00';

-- Query without time travel and you will see all the rows
SELECT * FROM local.warehouse.orders;
```

## Tagging 

```sql
-- get a snapshot_id
select committed_at, snapshot_id, manifest_list from local.warehouse.orders.snapshots;

-- Use your snapshot_id in as of version
ALTER TABLE local.warehouse.orders CREATE TAG `CHANGE-01` AS OF VERSION 3277809923527865161 RETAIN 10 DAYS;

INSERT INTO local.warehouse.orders VALUES 
('e481f51cbdc54678b7cc49136f2d6gh5',69,CAST('2023-11-21 09:56:33' AS TIMESTAMP));

-- you will see a difference of 1 row
SELECT COUNT(*) FROM local.warehouse.orders;
SELECT COUNT(*) FROM local.warehouse.orders VERSION AS OF 'CHANGE-01';
```
## Branching

```sql
-- Create 2 branches, that are both stored for 10 days
ALTER TABLE local.warehouse.orders_agg CREATE BRANCH `parallel-branch-v1` RETAIN 10 DAYS;
ALTER TABLE local.warehouse.orders_agg CREATE BRANCH `parallel-branch-v2` RETAIN 10 DAYS;

-- Use different logic for each of the branch

-- inserting into branch v1
INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch-v1`
SELECT date(order_date) as order_date, count(order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-03' GROUP BY 1;

INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch-v1`
SELECT date(order_date) as order_date, count(order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-04' GROUP BY 1;

-- inserting into branch v2
INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch-v2`
SELECT date(order_date) as order_date, count(distinct order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-03' GROUP BY 1;

INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch-v2`
SELECT date(order_date) as order_date, count(distinct order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-04' GROUP BY 1;

-- validate data, the v2 logic is correct
select * from local.warehouse.orders_agg.`branch_parallel-branch-v1` order by order_date;
select * from local.warehouse.orders_agg.`branch_parallel-branch-v2` order by order_date;

select * from local.warehouse.orders_agg order by order_date desc; 
-- push main branch to branch v2's state
CALL local.system.fast_forward('warehouse.orders_agg', 'main', 'parallel-branch-v2');

select * from local.warehouse.orders_agg order by order_date desc;
```

## Read from another system

Exit your docker using `exit`. From your project directory, in your terminal, run the following SQL command:

```sql
INSTALL iceberg;
LOAD iceberg;

WITH orders as (SELECT * FROM iceberg_scan('data/iceberg-warehouse/warehouse/orders', ALLOW_MOVED_PATHS=true))
select strftime(order_date, '%Y-%m-%d') as order_date
, count(distinct order_id) as num_orders
from orders 
group by strftime(order_date, '%Y-%m-%d') 
order by 1 desc;
```

In the above sql query, we use DuckDb to read Iceberg table and perform computations on it.
