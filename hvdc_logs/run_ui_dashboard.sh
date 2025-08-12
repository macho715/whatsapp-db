#!/bin/bash
duckdb -ui "$(dirname "$0")/notebooks/sla_kpi_dashboard.duckdbsql"
