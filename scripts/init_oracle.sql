-- Oracle XE Test Data Initialization
-- Creates tables with test data for data-absorb

-- Set schema to system
ALTER SESSION SET CURRENT_SCHEMA = SYSTEM;

-- Drop tables if exists (ignore errors if they don't exist)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE all_types CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE nullable_test CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE huge_table CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

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

-- Insert test data (1000 rows)
INSERT INTO all_types
SELECT 
    ROWNUM as id,
    MOD(ROWNUM, 100000),
    ROWNUM * 1000,
    MOD(ROWNUM, 1000),
    ROWNUM * 0.123456789,
    ROWNUM * 1.23456789,
    ROWNUM * 1.2345,
    'varchar_' || ROWNUM,
    'text_' || ROWNUM,
    MOD(ROWNUM, 2),
    TRUNC(SYSDATE) + MOD(ROWNUM, 365),
    SYSDATE + (MOD(ROWNUM, 100000) / 1440),
    NULL
FROM dual
CONNECT BY LEVEL <= 1000;

-- Insert nullable_test
INSERT INTO nullable_test
SELECT 
    ROWNUM as id,
    CASE WHEN MOD(ROWNUM,3) = 0 THEN NULL ELSE MOD(ROWNUM,100000) END,
    CASE WHEN MOD(ROWNUM,5) = 0 THEN NULL ELSE 'varchar_' || ROWNUM END,
    CASE WHEN MOD(ROWNUM,7) = 0 THEN NULL ELSE ROWNUM * 1.5 END,
    CASE WHEN MOD(ROWNUM,11) = 0 THEN NULL ELSE SYSDATE + (MOD(ROWNUM,100000) / 1440) END,
    CASE WHEN MOD(ROWNUM,13) = 0 THEN NULL ELSE MOD(ROWNUM,2) END
FROM dual
CONNECT BY LEVEL <= 1000;

-- Insert huge_table
INSERT INTO huge_table
SELECT 
    ROWNUM as id,
    'name_' || LPAD(ROWNUM, 7, '0'),
    ROWNUM * 0.01,
    MOD(ROWNUM, 2),
    SYSDATE + (MOD(ROWNUM,1000000) / 86400)
FROM dual
CONNECT BY LEVEL <= 1000;

COMMIT;

-- Verify counts
SELECT 'all_types' as table_name, COUNT(*) as row_count FROM all_types
UNION ALL
SELECT 'nullable_test', COUNT(*) FROM nullable_test
UNION ALL
SELECT 'huge_table', COUNT(*) FROM huge_table;