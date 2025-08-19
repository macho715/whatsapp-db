#!/usr/bin/env python3
"""
HVDC Logs Pipeline Runner
Bronze (JSONL) → Silver (Parquet) → Query Pipeline
"""

import duckdb
import os
import glob
from datetime import datetime
import json

class HVDCPipeline:
    def __init__(self, base_path="."):
        self.base_path = base_path
        self.bronze_path = os.path.join(base_path, "bronze", "2025", "08")
        self.silver_path = os.path.join(base_path, "silver", "logs")
        self.duckdb_path = os.path.join(base_path, "duckdb", "hvdc.duckdb")

        # Create directories if they don't exist
        os.makedirs(self.bronze_path, exist_ok=True)
        os.makedirs(self.silver_path, exist_ok=True)
        os.makedirs(os.path.dirname(self.duckdb_path), exist_ok=True)

        # Connect to DuckDB
        self.conn = duckdb.connect(self.duckdb_path)

    def run_transformation(self):
        """Run the complete transformation pipeline"""
        print("Starting HVDC Pipeline...")

        # Check bronze data
        jsonl_files = glob.glob(os.path.join(self.bronze_path, "*.jsonl"))
        if not jsonl_files:
            print("Warning: No JSONL files found in bronze directory")
            print(f"   Expected path: {self.bronze_path}")
            return False

        print(f"Found {len(jsonl_files)} JSONL files in bronze:")
        for file in jsonl_files:
            print(f"   - {os.path.basename(file)}")

        # Run transformation SQL
        try:
            sql_file = os.path.join(self.base_path, "transform.sql")
            if not os.path.exists(sql_file):
                print(f"Error: SQL file not found: {sql_file}")
                return False

            with open(sql_file, 'r') as f:
                sql_script = f.read()

            print("Running transformation...")
            self.conn.execute(sql_script)

            # Create/refresh a simple KPI view from silver table if available
            self.conn.execute(
                """
                CREATE OR REPLACE VIEW v_kpi_daily AS
                SELECT
                  CAST(date_gst AS DATE) AS date,
                  group_name,
                  COUNT(*) AS logs_count,
                  SUM(COALESCE(sla_breaches, 0)) AS total_sla_breaches
                FROM
                  sla_log
                GROUP BY 1,2
                """
            )

            # Check results using available tables
            result = self.conn.execute(
                """
                SELECT 'Bronze records' as layer, count(*) as count FROM raw_logs
                UNION ALL
                SELECT 'Silver records' as layer, count(*) as count FROM sla_log
                UNION ALL
                SELECT 'Parquet files' as layer, count(*) as count FROM read_parquet('silver/logs/**/*.parquet')
                """
            ).fetchdf()

            print("Transformation completed successfully!")
            print("\nPipeline Results:")
            for _, row in result.iterrows():
                print(f"   {row['layer']}: {row['count']}")

            return True

        except Exception as e:
            print(f"Error: Transformation failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def query_data(self, date_from=None, date_to=None, group_name=None):
        """Query the transformed data"""
        query = """
        SELECT date, group_name, logs_count, total_sla_breaches
        FROM v_kpi_daily
        WHERE 1=1
        """

        params = []
        if date_from:
            query += " AND date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND date <= ?"
            params.append(date_to)
        if group_name:
            query += " AND group_name LIKE ?"
            params.append(f"%{group_name}%")

        query += " ORDER BY date DESC, group_name"

        result = self.conn.execute(query, params).fetchdf()
        return result

    def export_to_csv(self, filename="kpi_report.csv"):
        """Export KPI data to CSV"""
        output_path = os.path.join(self.base_path, filename)

        query = """
        COPY (
            SELECT * FROM v_kpi_daily
        ) TO '{}' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        """.format(output_path.replace('\\', '/'))

        self.conn.execute(query)
        print(f"Exported to: {output_path}")

    def close(self):
        """Close database connection"""
        self.conn.close()

def main():
    """Main pipeline execution"""
    pipeline = HVDCPipeline()

    try:
        # Run transformation
        if pipeline.run_transformation():
            print("\nSample Queries:")
            print("1. All data:")
            result = pipeline.query_data()
            print(result.head())

            print("\n2. Export to CSV:")
            pipeline.export_to_csv()

            print("\n3. Date range query example:")
            result = pipeline.query_data(date_from="2025-08-01", date_to="2025-08-31")
            print(result.head())

    except Exception as e:
        print(f"Error: Pipeline execution failed: {e}")

    finally:
        pipeline.close()

if __name__ == "__main__":
    main()
