-- MariaDB Test Data Initialization
-- Creates tables with 1M+ rows for testing data-absorb

USE testdb;

-- Drop tables if exists
DROP TABLE IF EXISTS all_types;
DROP TABLE IF EXISTS nullable_test;
DROP TABLE IF EXISTS huge_table;

-- Table 1: all_types - contains all common data types
CREATE TABLE all_types (
    id BIGINT PRIMARY KEY,
    col_int INT,
    col_bigint BIGINT,
    col_smallint SMALLINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_decimal DECIMAL(18,4),
    col_varchar VARCHAR(500),
    col_text TEXT,
    col_bool BOOLEAN,
    col_date DATE,
    col_timestamp DATETIME,
    col_blob BLOB
) ENGINE=InnoDB;

-- Set session for faster inserts
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;
SET SESSION sync_binlog = 0;

-- Insert 1 million rows using stored procedure
DELIMITER //

DROP PROCEDURE IF EXISTS insert_all_types//

CREATE PROCEDURE insert_all_types()
BEGIN
    DECLARE i BIGINT DEFAULT 1;
    DECLARE batch_size INT DEFAULT 10000;
    DECLARE total_rows BIGINT DEFAULT 1000000;
    
    WHILE i <= total_rows DO
        INSERT INTO all_types VALUES (
            i,
            i % 100000,
            i * 1000,
            i % 1000,
            i * 0.123456789,
            i * 1.23456789,
            i * 1.2345,
            CONCAT('varchar_', i),
            CONCAT('text_', REPEAT('x', LEAST(i % 200, 200)), i),
            i % 2 = 0,
            DATE_ADD('2024-01-01', INTERVAL (i % 365) DAY),
            TIMESTAMPADD(MINUTE, i % 100000, '2024-01-01 00:00:00'),
            UNHEX(MD5(i))
        );
        SET i = i + 1;
    END WHILE;
END//

DELIMITER ;

-- Call the procedure (will take a while)
-- CALL insert_all_types();

-- Alternative: Use INSERT with generate sequence (faster)
-- For MariaDB, we use a different approach with recursive CTE
INSERT INTO all_types
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 1000000
)
SELECT 
    n,
    n % 100000,
    n * 1000,
    n % 1000,
    n * 0.123456789,
    n * 1.23456789,
    n * 1.2345,
    CONCAT('varchar_', n),
    CONCAT('text_', REPEAT('x', 50), n),
    n % 2 = 0,
    DATE_ADD('2024-01-01', INTERVAL (n % 365) DAY),
    TIMESTAMPADD(MINUTE, n % 100000, '2024-01-01 00:00:00'),
    UNHEX(MD5(n))
FROM nums;

-- Table 2: nullable_test - tests NULL handling
CREATE TABLE nullable_test (
    id BIGINT PRIMARY KEY,
    col_int INT,
    col_varchar VARCHAR(200),
    col_decimal DECIMAL(10,2),
    col_timestamp DATETIME,
    col_bool BOOLEAN
) ENGINE=InnoDB;

INSERT INTO nullable_test
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 1000000
)
SELECT 
    n,
    CASE WHEN n % 3 = 0 THEN NULL ELSE n % 100000 END,
    CASE WHEN n % 5 = 0 THEN NULL ELSE CONCAT('varchar_', n) END,
    CASE WHEN n % 7 = 0 THEN NULL ELSE n * 1.5 END,
    CASE WHEN n % 11 = 0 THEN NULL ELSE TIMESTAMPADD(MINUTE, n % 100000, '2024-01-01 00:00:00') END,
    CASE WHEN n % 13 = 0 THEN NULL ELSE n % 2 = 0 END
FROM nums;

-- Table 3: huge_table - single large table for performance testing
CREATE TABLE huge_table (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    value DECIMAL(15,2),
    flag BOOLEAN,
    created_at DATETIME
) ENGINE=InnoDB;

INSERT INTO huge_table
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 1000000
)
SELECT 
    n,
    CONCAT('name_', LPAD(n, 7, '0')),
    n * 0.01,
    n % 2 = 0,
    TIMESTAMPADD(SECOND, n % 1000000, '2024-01-01 00:00:00')
FROM nums
OPTION (MAX_STATEMENT_TIME = 3600);

-- Verify row counts
SELECT 'all_types' as table_name, COUNT(*) as row_count FROM all_types
UNION ALL
SELECT 'nullable_test', COUNT(*) FROM nullable_test
UNION ALL
SELECT 'huge_table', COUNT(*) FROM huge_table;