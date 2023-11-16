# [WIP] iceberg-features


```bash
# Start docker and sh into it
rm -rf ./data
docker compose down
docker compose up --build -d
docker exec -ti local-spark bash
```

## Create spark table

```bash
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

('47770eb9100c2d0c44946d9cf07ec65d',26,'on_route',CAST('2023-11-03 12:56:33' AS TIMESTAMP)),
('47770eb9100c2d0c44946d9cf07ec65d',99,'lost',CAST('2023-11-03 13:56:33' AS TIMESTAMP)),

('949d5b44dbf5de918fe9c16f97b45f8a',35,'delivered',CAST('2023-11-04 09:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',5,'lost',CAST('2023-11-04 10:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',105,'lost',CAST('2023-11-04 11:56:33' AS TIMESTAMP)),

INSERT INTO local.warehouse.orders VALUES
('ad21c59c0840e6cb83a9ceb5573f8159',23,'delivered',CAST('2023-11-05 04:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',12,'on_route',CAST('2023-11-05 08:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',19,'delivered',CAST('2023-11-05 10:56:33' AS TIMESTAMP));

INSERT INTO local.warehouse.orders VALUES
('a4591c265e18cb1dcee52889e2d8acc3',82,'lost',CAST('2023-11-06 10:45:33' AS TIMESTAMP)),
('a4591c265e18cb1dcee52889e2d8acc3',1234,'on_route',CAST('2023-11-06 12:45:33' AS TIMESTAMP));

-- Hidden partition
EXPLAIN SELECT cust_id, order_date FROM local.warehouse.orders WHERE order_date BETWEEN '2023-11-01 12:45:33' AND '2023-11-06 12:45:33';

-- schema evolution
ALTER TABLE local.warehouse.orders ALTER COLUMN cust_id TYPE bigint; -- Change 1
ALTER TABLE local.warehouse.orders DROP COLUMN order_status; -- Change 2

-- parititon evolution
ALTER TABLE local.warehouse.orders ADD PARTITION FIELD cust_id; -- Change 3

-- show manifest files & changing schema 
-- check snapshots

select * from local.warehouse.orders.files;
select * from local.warehouse.orders.history;
select committed_at, snapshot_id, manifest_list from local.warehouse.orders.snapshots;
select * from local.warehouse.orders.manifests;
select * from local.warehouse.orders.metadata_log_entries;
select * from local.warehouse.orders.files;

INSERT INTO local.warehouse.orders VALUES 
('e481f51cbdc54678b7cc49136f2d6af7',69,CAST('2023-11-14 09:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',87,CAST('2023-11-14 10:56:33' AS TIMESTAMP));

select * from local.warehouse.orders.files;
select * from local.warehouse.orders.history;
select * from local.warehouse.orders.snapshots;
select * from local.warehouse.orders.manifests;

-- time travel
SELECT * FROM local.warehouse.orders TIMESTAMP AS OF '2023-11-14 12:00:00.00'; -- before adding the 2 new rows

-- tagging
ALTER TABLE local.warehouse.orders CREATE TAG `CHANGE-01` AS OF VERSION 3277809923527865161 RETAIN 10 DAYS;

SELECT COUNT(*) FROM local.warehouse.orders; -- 17
SELECT COUNT(*) FROM local.warehouse.orders VERSION AS OF 'CHANGE-01'; -- 15

-- dev: branching
ALTER TABLE local.warehouse.orders SET TBLPROPERTIES (
    'write.wap.enabled'='true'
);

-- branching
DROP TABLE IF EXISTS local.warehouse.orders_agg;
CREATE TABLE local.warehouse.orders_agg (
    order_date timestamp,
    num_orders INT
) USING iceberg
TBLPROPERTIES ('write.wap.enabled'='true');

INSERT INTO local.warehouse.orders_agg
SELECT date(order_date) as order_date, count(order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-01' GROUP BY 1;

INSERT INTO local.warehouse.orders_agg
SELECT date(order_date) as order_date, count(order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-02' GROUP BY 1;

select * from local.warehouse.orders_agg;

-- Create branch
ALTER TABLE local.warehouse.orders_agg CREATE BRANCH `parallel-branch` RETAIN 7 DAYS;
ALTER TABLE local.warehouse.orders_agg CREATE BRANCH `parallel-branch-v2` RETAIN 7 DAYS;

-- tag table before parallel runs
-- inserting into table
INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch`
SELECT date(order_date) as order_date, count(order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-03' GROUP BY 1;

INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch`
SELECT date(order_date) as order_date, count(order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-04' GROUP BY 1;

-- inserting into parallel branch
INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch-v2`
SELECT date(order_date) as order_date, count(distinct order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-03' GROUP BY 1;

INSERT INTO local.warehouse.orders_agg.`branch_parallel-branch-v2`
SELECT date(order_date) as order_date, count(distinct order_id) as num_orders from local.warehouse.orders WHERE date(order_date) = '2023-11-04' GROUP BY 1;

-- validate data is same

select * from local.warehouse.orders_agg.`branch_parallel-branch` order by order_date;
select * from local.warehouse.orders_agg.`branch_parallel-branch-v2` order by order_date;
-- duplicate order_id

-- push main to head of parallel 
CALL local.system.fast_forward('warehouse.orders_agg', 'main', 'parallel-branch-v2');
select * from local.warehouse.orders_agg order by order_date desc;

-- read from python & duckdb
```

```sh
pip install "pyiceberg[s3fs,hive]"
brew install duckdb
```

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