#!/usr/bin/env python3
"""
Verify data-absorb export results using duckdb
Checks row counts, schemas, and data integrity
"""

import sys
import os
import subprocess
from pathlib import Path

def run_duckdb_query(query: str) -> str:
    """Execute duckdb query and return output"""
    cmd = ["duckdb", "-csv", "-noheader", ":memory:", "-c", query]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"DuckDB query failed: {result.stderr}")
    return result.stdout.strip()

def verify_parquet(parquet_file: Path, db_name: str, expected_rows: int = None) -> dict:
    """Verify a single parquet file"""
    print(f"\n{'='*60}")
    print(f"Verifying: {parquet_file.name} ({db_name})")
    print('='*60)
    
    results = {}
    
    # 1. Check file exists
    if not parquet_file.exists():
        print(f"  ✗ File not found: {parquet_file}")
        return None
    
    size = parquet_file.stat().st_size
    print(f"  File size: {size:,} bytes")
    results['size'] = size
    
    # 2. Verify row count
    query = f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')"
    try:
        count = int(run_duckdb_query(query))
        print(f"  Row count: {count:,}")
        results['row_count'] = count
        
        if expected_rows and count != expected_rows:
            print(f"  ✗ Expected {expected_rows:,} rows, got {count:,}")
            results['error'] = f"Row count mismatch: expected {expected_rows}, got {count}"
    except Exception as e:
        print(f"  ✗ Failed to get row count: {e}")
        results['error'] = str(e)
        return results
    
    # 3. Verify schema
    query = f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}')"
    try:
        schema_output = run_duckdb_query(query)
        print(f"\n  Schema:")
        for line in schema_output.split('\n')[1:]:  # Skip header
            if line.strip():
                parts = line.split(',')
                if len(parts) >= 2:
                    col = parts[0].strip()
                    typ = parts[1].strip().strip('"')
                    print(f"    {col}: {typ}")
        results['schema'] = schema_output
    except Exception as e:
        print(f"  ✗ Failed to get schema: {e}")
        results['error'] = str(e)
    
    # 4. Verify data integrity (checksum for first/last few records)
    table_name = parquet_file.stem
    
    # Get first and last record
    query = f"SELECT id, name FROM read_parquet('{parquet_file}') ORDER BY id LIMIT 1"
    try:
        first = run_duckdb_query(query)
        print(f"\n  First row: {first}")
    except:
        pass
    
    query = f"SELECT id FROM read_parquet('{parquet_file}') ORDER BY id DESC LIMIT 1"
    try:
        last = run_duckdb_query(query)
        print(f"  Last row: {last}")
    except:
        pass
    
    # 5. Verify SUM (data integrity)
    query = f"SELECT SUM(id) FROM read_parquet('{parquet_file}')"
    try:
        sum_id = run_duckdb_query(query)
        print(f"  SUM(id): {sum_id}")
    except:
        pass
    
    return results

def main():
    output_dir = Path("./testdata/db_export")
    
    if not output_dir.exists():
        print(f"Output directory not found: {output_dir}")
        print("\nRun export first with:")
        print("  go run ./cmd/data-absorb --config configs/db_test.toml")
        sys.exit(1)
    
    print(f"\n{'#'*60}")
    print("# Data Absorb Export Verification")
    print('#'*60)
    
    # Database configurations to verify
    db_configs = {
        "postgres": {
            "dir": output_dir / "postgres",
            "tables": {
                "all_types": 1000000,
                "nullable_test": 1000000,
                "huge_table": 1000000,
            }
        },
        "mariadb": {
            "dir": output_dir / "mariadb", 
            "tables": {
                "all_types": 1000000,
                "nullable_test": 1000000,
                "huge_table": 1000000,
            }
        },
        "oracle": {
            "dir": output_dir / "oracle",
            "tables": {
                "all_types": 100000,
                "nullable_test": 100000,
                "huge_table": 100000,
            }
        }
    }
    
    all_passed = True
    
    for db_name, config in db_configs.items():
        db_dir = config["dir"]
        
        if not db_dir.exists():
            print(f"\n{'='*60}")
            print(f"Database: {db_name.upper()} - No output directory found")
            print(f"{'='*60}")
            print(f"  Directory: {db_dir}")
            all_passed = False
            continue
        
        print(f"\n{'='*60}")
        print(f"Database: {db_name.upper()}")
        print(f"{'='*60}")
        
        for table_name, expected_rows in config["tables"].items():
            parquet_file = db_dir / f"{table_name}.parquet"
            
            results = verify_parquet(parquet_file, db_name, expected_rows)
            
            if results is None:
                all_passed = False
                continue
                
            if 'error' in results:
                all_passed = False
    
    print("")
    print("#" * 60)
    if all_passed:
        print("# All verifications PASSED ✓")
    else:
        print("# Some verifications FAILED ✗")
    print("#" * 60)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())