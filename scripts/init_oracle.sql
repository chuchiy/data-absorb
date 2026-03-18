-- Oracle XE Test Data Initialization
-- Creates tables with 1M+ rows for testing data-absorb

-- Connect as system user
CONNECT system/oraclepass@//localhost/XE

-- Create testuser and grant privileges
ALTER SESSION SET CURRENT_SCHEMA = SYSTEM;

-- Drop tables if exists
DROP TABLE all_types CASCADE;
DROP TABLE nullable_test CASCADE;
DROP TABLE huge_table CASCADE;

-- Create tables
CREATE TABLE all_types (
    id NUMBER(19) PRIMARY KEY,
    col_int NUMBER(10),
    col_bigint NUMBER(19),
    col_smallint NUMBER(5),
    col_float FLOAT,
    col_double DOUBLE PRECISION,
    col_decimal NUMBER(18,4),
    col_varchar VARCHAR2(500),
    col_text CLOB,
    col_bool NUMBER(1),
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_blob BLOB
);

CREATE TABLE nullable_test (
    id NUMBER(19) PRIMARY KEY,
    col_int NUMBER(10),
    col_varchar VARCHAR2(200),
    col_decimal NUMBER(10,2),
    col_timestamp TIMESTAMP,
    col_bool NUMBER(1)
);

CREATE TABLE huge_table (
    id NUMBER(19) PRIMARY KEY,
    name VARCHAR2(100),
    value NUMBER(15,2),
    col_bool NUMBER(1),
    created_at TIMESTAMP
);

-- Insert data using PL/SQL (will take a while for 1M rows)
-- For testing, we insert 100k rows first to verify, then can increase

DECLARE
    batch_size NUMBER := 10000;
    total_rows NUMBER := 100000;  -- Start with 100k for initial test
BEGIN
    -- Insert all_types
    FOR i IN 1..total_rows LOOP
        INSERT INTO all_types VALUES (
            i,
            MOD(i, 100000),
            i * 1000,
            MOD(i, 1000),
            i * 0.123456789,
            i * 1.23456789,
            i * 1.2345,
            'varchar_' || i,
            'text_' || RPAD('x', 50, 'x') || i,
            MOD(i, 2),
            TRUNC(SYSDATE) + MOD(i, 365),
            SYSDATE + (MOD(i, 100000) / 1440,
            UTL_RAW.CAST_TO_RAW(DBMS_OBFUSCATION_TOOLKIT.MD5(INPUT_STRING => TO_CHAR(i)))
        );
        
        IF MOD(i, batch_size) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    COMMIT;
    
    -- Insert nullable_test
    FOR i IN 1..total_rows LOOP
        INSERT INTO nullable_test VALUES (
            i,
            CASE WHEN MOD(i,3) = 0 THEN NULL ELSE MOD(i,100000) END,
            CASE WHEN MOD(i,5) = 0 THEN NULL ELSE 'varchar_' || i END,
            CASE WHEN MOD(i,7) = 0 THEN NULL ELSE i * 1.5 END,
            CASE WHEN MOD(i,11) = 0 THEN NULL ELSE SYSDATE + (MOD(i,100000) / 1440 END,
            CASE WHEN MOD(i,13) = 0 THEN NULL ELSE MOD(i,2) END
        );
        
        IF MOD(i, batch_size) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    COMMIT;
    
    -- Insert huge_table
    FOR i IN 1..total_rows LOOP
        INSERT INTO huge_table VALUES (
            i,
            'name_' || LPAD(i, 7, '0'),
            i * 0.01,
            MOD(i, 2),
            SYSDATE + (MOD(i,1000000) / 86400)
        );
        
        IF MOD(i, batch_size) = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Inserted ' || total_rows || ' rows into each table');
END;
/

-- Verify counts
SELECT 'all_types' as table_name, COUNT(*) as row_count FROM all_types
UNION ALL
SELECT 'nullable_test', COUNT(*) FROM nullable_test
UNION ALL
SELECT 'huge_table', COUNT(*) FROM huge_table;

-- Show table sizes
SELECT segment_name, bytes / 1024 / 1024 as size_mb
FROM user_segments
WHERE segment_type = 'TABLE'
ORDER BY bytes DESC;