-- JDBC catalog backing store for the Iceberg S3FileIO cases. The table data
-- lives in MinIO; PostgreSQL only holds catalog metadata.
SELECT 'CREATE DATABASE iceberg_catalog'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg_catalog')\gexec
