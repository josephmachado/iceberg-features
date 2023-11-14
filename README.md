# [WIP] iceberg-features


```bash
# Start docker and sh into it
docker compose down
docker compose up --build -d
docker exec -ti local-spark bash
```

## Create spark table

```bash
# Start spark without iceberg
spark-sql --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/spark-warehouse
```

```sql
CREATE SCHEMA warehouse;
DROP TABLE IF EXISTS warehouse.orders;
CREATE TABLE warehouse.orders (
    order_id string,
    cust_id INT,
    order_status string,
    order_date timestamp
);

INSERT INTO warehouse.orders VALUES 
('e481f51cbdc54678b7cc49136f2d6af7',69,'delivered', CAST('2023-11-01 09:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',87,'delivered',CAST('2023-11-01 10:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',125,'delivered',CAST('2023-11-01 11:56:33' AS TIMESTAMP)),

('53cdb2fc8bc7dce0b6741e2150273451',17,'delivered',CAST('2023-11-02 11:56:33' AS TIMESTAMP)),
('53cdb2fc8bc7dce0b6741e2150273451',19,'on_route',CAST('2023-11-02 12:56:33' AS TIMESTAMP)),

('47770eb9100c2d0c44946d9cf07ec65d',26,'on_route',CAST('2023-11-03 12:56:33' AS TIMESTAMP)),
('47770eb9100c2d0c44946d9cf07ec65d',99,'lost',CAST('2023-11-03 13:56:33' AS TIMESTAMP)),

('949d5b44dbf5de918fe9c16f97b45f8a',35,'delivered',CAST('2023-11-04 09:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',5,'lost',CAST('2023-11-04 10:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',105,'lost',CAST('2023-11-04 11:56:33' AS TIMESTAMP)),

('ad21c59c0840e6cb83a9ceb5573f8159',23,'delivered',CAST('2023-11-05 04:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',12,'on_route',CAST('2023-11-05 08:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',19,'delivered',CAST('2023-11-05 10:56:33' AS TIMESTAMP)),

('a4591c265e18cb1dcee52889e2d8acc3',82,'lost',CAST('2023-11-06 10:45:33' AS TIMESTAMP)),
('a4591c265e18cb1dcee52889e2d8acc3',1234,'on_route',CAST('2023-11-06 12:45:33' AS TIMESTAMP));
exit;
```

## Create iceberg table
```bash
# Start spark sql with iceberg and local warehouse
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/iceberg-warehouse
```

```sql
-- CREATE SCHEMA warehouse;
-- Use Apache Iceberg table format
DROP TABLE IF EXISTS local.warehouse.orders_iceberg;
CREATE TABLE local.warehouse.orders_iceberg (
    order_id string,
    cust_id INT,
    order_status string,
    order_date timestamp
) USING iceberg
PARTITIONED BY (date(order_date));

INSERT INTO local.warehouse.orders_iceberg VALUES 
('e481f51cbdc54678b7cc49136f2d6af7',69,'delivered',CAST('2023-11-01 09:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',87,'delivered',CAST('2023-11-01 10:56:33' AS TIMESTAMP)),
('e481f51cbdc54678b7cc49136f2d6af7',125,'delivered',CAST('2023-11-01 11:56:33' AS TIMESTAMP)),

('53cdb2fc8bc7dce0b6741e2150273451',17,'delivered',CAST('2023-11-02 11:56:33' AS TIMESTAMP)),
('53cdb2fc8bc7dce0b6741e2150273451',19,'on_route',CAST('2023-11-02 12:56:33' AS TIMESTAMP)),

('47770eb9100c2d0c44946d9cf07ec65d',26,'on_route',CAST('2023-11-03 12:56:33' AS TIMESTAMP)),
('47770eb9100c2d0c44946d9cf07ec65d',99,'lost',CAST('2023-11-03 13:56:33' AS TIMESTAMP)),

('949d5b44dbf5de918fe9c16f97b45f8a',35,'delivered',CAST('2023-11-04 09:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',5,'lost',CAST('2023-11-04 10:56:33' AS TIMESTAMP)),
('949d5b44dbf5de918fe9c16f97b45f8a',105,'lost',CAST('2023-11-04 11:56:33' AS TIMESTAMP)),

('ad21c59c0840e6cb83a9ceb5573f8159',23,'delivered',CAST('2023-11-05 04:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',12,'on_route',CAST('2023-11-05 08:56:33' AS TIMESTAMP)),
('ad21c59c0840e6cb83a9ceb5573f8159',19,'delivered',CAST('2023-11-05 10:56:33' AS TIMESTAMP)),

('a4591c265e18cb1dcee52889e2d8acc3',82,'lost',CAST('2023-11-06 10:45:33' AS TIMESTAMP)),
('a4591c265e18cb1dcee52889e2d8acc3',1234,'on_route',CAST('2023-11-06 12:45:33' AS TIMESTAMP));

SELECT order_status
, count(*) as cnt
FROM warehouse.orders
GROUP BY 1;

SELECT order_status
, count(*) as cnt
FROM local.warehouse.orders_iceberg
GROUP BY 1;

-- SCHEMA CHANGE

-- ADD
ALTER TABLE warehouse.orders ADD COLUMNS (order_name string comment 'order_name docs');
-- DELETE COL
-- order_status
ALTER TABLE warehouse.orders SET TBLPROPERTIES ('transactional'='true');
ALTER TABLE warehouse.orders DROP COLUMN order_status;
ALTER TABLE local.warehouse.orders_iceberg DROP COLUMN order_status;

-- CHANGE COL TYPE
-- cust_id to string
-- ALTER TABLE warehouse.orders ALTER COLUMN cust_id TYPE bigint; 
ALTER TABLE local.warehouse.orders_iceberg ALTER COLUMN cust_id TYPE bigint;


SELECT * FROM local.warehouse.orders LIMIT 5;
SELECT * FROM local.warehouse.orders_iceberg LIMIT 5;

-- CHANGE COLUMN TYPE
-- DROP COLUMN

-- hidden partition issue
EXPLAIN SELECT cust_id, order_date FROM local.warehouse.orders_iceberg WHERE order_date BETWEEN '2023-11-01 12:45:33' AND '2023-11-06 12:45:33';


EXPLAIN SELECT cust_id, order_date FROM local.warehouse.orders_iceberg WHERE order_date ='2023-11-01';

EXPLAIN SELECT order_date, count(*) as cnt FROM local.warehouse.orders_iceberg WHERE order_date BETWEEN '2023-11-01 12:45:33' AND '2023-11-06 12:45:33' GROUP BY 1;

== Physical Plan ==
*(1) Filter ((isnotnull(order_date#113) AND (order_date#113 >= 2023-11-01 12:45:33)) AND (order_date#113 <= 2023-11-06 12:45:33))
+- *(1) ColumnarToRow
   +- BatchScan[cust_id#111, order_date#113] local.warehouse.orders_iceberg (branch=null) [filters=order_date IS NOT NULL, order_date >= 1698842733000000, order_date <= 1699274733000000, groupedBy=] RuntimeFilters: []

*(1) Filter ((order_date#37 >= 2023-11-01 12:45:33) AND (order_date#37 <= 2023-11-06 12:45:33))
+- *(1) ColumnarToRow
   +- BatchScan[order_id#34, cust_id#35, order_status#36, order_date#37] local.warehouse.orders_iceberg (branch=null) [filters=order_date IS NOT NULL, order_date >= 1698842733000000, order_date <= 1699274733000000, groupedBy=] RuntimeFilters: []

-- bigger data size?
EXPLAIN SELECT * FROM local.warehouse.orders_iceberg WHERE order_date = '2023-11-01';

-- alter partition
ALTER TABLE  local.warehouse.orders_iceberg ADD PARTITION FIELD cust_id;
-- ALTER TABLE warehouse.orders ADD PARTITION FIELD cust_id;

-- tagging

-- add tag
--insert data
-- check previous version

-- branching
-- branch to parallel run with avg
-- run n times
-- push master to head of parallel 
```

Read with Python
