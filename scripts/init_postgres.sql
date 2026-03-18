-- PostgreSQL Test Data Initialization
-- Creates tables with 1M+ rows for testing data-absorb

-- Drop tables if exists
DROP TABLE IF EXISTS all_types CASCADE;
DROP TABLE IF EXISTS nullable_test CASCADE;
DROP TABLE IF EXISTS huge_table CASCADE;

-- Table 1: all_types - contains all common data types
CREATE TABLE all_types (
    id BIGINT PRIMARY KEY,
    col_int INTEGER,
    col_bigint BIGINT,
    col_smallint SMALLINT,
    col_float FLOAT,
    col_double DOUBLE PRECISION,
    col_decimal DECIMAL(18,4),
    col_varchar VARCHAR(500),
    col_text TEXT,
    col_bool BOOLEAN,
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_blob BYTEA
);

-- Insert 1 million rows using generate_series
INSERT INTO all_types (
    id, col_int, col_bigint, col_smallint, col_float, col_double,
    col_decimal, col_varchar, col_text, col_bool, col_date, col_timestamp, col_blob
)
SELECT 
    i,
    i % 100000,
    i * 1000,
    i % 1000,
    i * 0.123456789::float8,
    i * 1.23456789::float8,
    (i * 1.2345)::decimal(18,4),
    'varchar_' || i,
    'text_' || repeat('x', LEAST(i % 200, 200)) || i,
    i % 2 = 0,
    DATE '2024-01-01' + (i % 365) * INTERVAL '1 day',
    TIMESTAMP '2024-01-01 00:00:00' + (i % 100000) * INTERVAL '1 minute',
    md5(i::text)::bytea
FROM generate_series(1, 1000000) i;

-- Table 2: nullable_test - tests NULL handling
CREATE TABLE nullable_test (
    id BIGINT PRIMARY KEY,
    col_int INTEGER,
    col_varchar VARCHAR(200),
    col_decimal DECIMAL(10,2),
    col_timestamp TIMESTAMP,
    col_bool BOOLEAN
);

INSERT INTO nullable_test (id, col_int, col_varchar, col_decimal, col_timestamp, col_bool)
SELECT 
    i,
    CASE WHEN i % 3 = 0 THEN NULL ELSE i % 100000 END,
    CASE WHEN i % 5 = 0 THEN NULL ELSE 'varchar_' || i END,
    CASE WHEN i % 7 = 0 THEN NULL ELSE (i * 1.5)::decimal(10,2) END,
    CASE WHEN i % 11 = 0 THEN NULL ELSE TIMESTAMP '2024-01-01 00:00:00' + (i % 100000) * INTERVAL '1 minute' END,
    CASE WHEN i % 13 = 0 THEN NULL ELSE i % 2 = 0 END
FROM generate_series(1, 1000000) i;

-- Table 3: huge_table - single large table for performance testing
CREATE TABLE huge_table (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    value DECIMAL(15,2),
    flag BOOLEAN,
    created_at TIMESTAMP
);

INSERT INTO huge_table (id, name, value, flag, created_at)
SELECT 
    i,
    'name_' || lpad(i::text, 7, '0'),
    (i * 0.01)::decimal(15,2),
    i % 2 = 0,
    TIMESTAMP '2024-01-01 00:00:00' + (i % 1000000) * INTERVAL '1 second'
FROM generate_series(1, 1000000) i;

-- Create indexes for better query performance
CREATE INDEX idx_all_types_int ON all_types(col_int);
CREATE INDEX idx_all_types_timestamp ON all_types(col_timestamp);
CREATE INDEX idx_nullable_test_int ON nullable_test(col_int);
CREATE INDEX idx_huge_table_value ON huge_table(value);

-- Verify row counts
SELECT 'all_types' as table_name, COUNT(*) as row_count FROM all_types
UNION ALL
SELECT 'nullable_test', COUNT(*) FROM nullable_test
UNION ALL
SELECT 'huge_table', COUNT(*) FROM huge_table;

-- Show table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;