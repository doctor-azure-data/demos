-- Step 1: Identify High-Cost or Inefficient Queries
SELECT query_id, execution_time, total_bytes_scanned, credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
ORDER BY total_bytes_scanned DESC;

-- Step 2: Query Optimization Opportunities
SELECT query_text, plan_id, plan_step_id, operation, estimated_performed_bytes
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY('<query_id>'));

-- Step 3: Resource Utilization
SELECT name, warehouse_size, max_cluster_count
FROM INFORMATION_SCHEMA.WAREHOUSES
WHERE state = 'RUNNING'
  AND (current_running_sessions = 0 OR current_cluster_count > max_cluster_count);

-- Step 4: Table Scans and Data Storage
SELECT table_schema, table_name, total_scan_bytes
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STATISTICS
WHERE total_scan_bytes > <threshold>;

-- Step 5: Materialized Views
CREATE MATERIALIZED VIEW <mv_name>
AS
SELECT ...
FROM ...
WHERE ...;

-- Step 6: Query Caching
ALTER WAREHOUSE <warehouse_name> SET QUERY_CACHING_ENABLED = TRUE;

SELECT query_id, query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE execution_count > <threshold>;

