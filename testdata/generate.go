package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	dbPath := "/home/dht/agentic/data-absorb/testdata/test.db"
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		fmt.Println("Warning: could not remove old db:", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := createTables(db); err != nil {
		fmt.Println("Error creating tables:", err)
		os.Exit(1)
	}

	if err := insertTestData(db); err != nil {
		fmt.Println("Error inserting data:", err)
		os.Exit(1)
	}

	fmt.Println("Test database created successfully!")
}

func createTables(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE test_types (
			id INTEGER PRIMARY KEY,
			c_int INTEGER,
			c_bigint BIGINT,
			c_smallint SMALLINT,
			c_tinyint TINYINT,
			c_float REAL,
			c_double DOUBLE,
			c_decimal DECIMAL(10,4),
			c_varchar VARCHAR(100),
			c_text TEXT,
			c_char CHAR(10),
			c_bool BOOLEAN,
			c_date DATE,
			c_time TIME,
			c_timestamp DATETIME,
			c_blob BLOB,
			c_json TEXT,
			c_null INTEGER
		)`,
		`CREATE TABLE test_nulls (
			id INTEGER PRIMARY KEY,
			v_int INTEGER,
			v_bigint BIGINT,
			v_float REAL,
			v_varchar VARCHAR(100),
			v_text TEXT,
			v_bool BOOLEAN,
			v_date DATE,
			v_timestamp DATETIME,
			v_blob BLOB,
			v_json TEXT
		)`,
		`CREATE TABLE test_empty (
			id INTEGER PRIMARY KEY,
			name TEXT,
			value INTEGER
		)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("execute %s: %w", q, err)
		}
	}
	return nil
}

func insertTestData(db *sql.DB) error {
	queries := []string{
		`INSERT INTO test_types (id, c_int, c_bigint, c_smallint, c_tinyint, c_float, c_double, c_decimal, c_varchar, c_text, c_char, c_bool, c_date, c_time, c_timestamp, c_blob, c_json, c_null)
			VALUES (1, 42, 123456789012345, 100, 10, 3.14159, 3.14159265358979, 123.4567, 'hello', 'long text content', 'char val', 1, '2024-01-15', '12:30:45', '2024-01-15 12:30:45', X'01020304', '{"key":"value"}', NULL)`,
		`INSERT INTO test_types (id, c_int, c_bigint, c_smallint, c_tinyint, c_float, c_double, c_decimal, c_varchar, c_text, c_char, c_bool, c_date, c_time, c_timestamp, c_blob, c_json, c_null)
			VALUES (2, -42, -123456789012345, -100, -10, -3.14159, -3.14159265358979, -123.4567, 'world', 'another text', 'abc', 0, '2023-12-31', '23:59:59', '2023-12-31 23:59:59', X'05060708', '{"nested":{"a":1}}', 999)`,
		`INSERT INTO test_types (id, c_int, c_bigint, c_smallint, c_tinyint, c_float, c_double, c_decimal, c_varchar, c_text, c_char, c_bool, c_date, c_time, c_timestamp, c_blob, c_json, c_null)
			VALUES (3, 0, 0, 0, 0, 0.0, 0.0, 0.0, '', '', '', 0, '1970-01-01', '00:00:00', '1970-01-01 00:00:00', NULL, NULL, 0)`,
		`INSERT INTO test_nulls (id, v_int, v_bigint, v_float, v_varchar, v_text, v_bool, v_date, v_timestamp, v_blob, v_json)
			VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`,
		`INSERT INTO test_nulls (id, v_int, v_bigint, v_float, v_varchar, v_text, v_bool, v_date, v_timestamp, v_blob, v_json)
			VALUES (2, 100, 9999999999, 1.5, 'not null', 'text value', 1, '2024-01-01', '2024-01-01 10:00:00', X'abcd', '{"test":true}')`,
		`INSERT INTO test_nulls (id, v_int, v_bigint, v_float, v_varchar, v_text, v_bool, v_date, v_timestamp, v_blob, v_json)
			VALUES (3, NULL, 500, NULL, 'mixed', NULL, NULL, '2024-02-01', NULL, X'1122', NULL)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("execute %s: %w", q, err)
		}
	}
	return nil
}
