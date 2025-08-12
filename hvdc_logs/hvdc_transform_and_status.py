import requests
import duckdb
import pandas as pd
from datetime import datetime

# ===== 설정 =====
API_URL = "http://127.0.0.1:8004"          # API 서버 주소
API_KEY = "dev"                            # API 키
DB_PATH = r"C:\cursor-mcp\whatsapp db\data\whatsapp.db"  # DuckDB DB 파일 경로

def run_transform():
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 🚀 Running /hvdc/transform ...")
    resp = requests.post(f"{API_URL}/hvdc/transform", headers={"X-API-Key": API_KEY})
    if resp.status_code == 200:
        data = resp.json()
        print("✅ Transform executed:", data.get("status"), f"(Processed: {data.get('processed')})")
        return True
    else:
        print("❌ Transform failed:", resp.status_code, resp.text)
        return False

def show_duckdb_status():
    con = duckdb.connect(DB_PATH)

    version = con.execute("PRAGMA version").fetchone()[0]
    db_list = con.execute("PRAGMA database_list").fetchdf()
    settings = con.execute("""
        SELECT name, value
        FROM pragma_settings()
        WHERE name IN ('threads','max_memory','default_order')
    """).fetchdf()
    storage_info = con.execute("""
        SELECT schema, table, total_blocks, total_size, free_blocks
        FROM pragma_storage_info()
        ORDER BY total_size DESC
        LIMIT 10
    """).fetchdf()
    table_counts = []
    for tbl in con.execute("PRAGMA show_tables").fetchall():
        tbl_name = tbl[0]
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM \"{tbl_name}\"").fetchone()[0]
            table_counts.append({"table": tbl_name, "count": cnt})
        except:
            pass
    table_counts_df = pd.DataFrame(table_counts).sort_values(by="count", ascending=False).head(10)

    # 최근 KPI 데이터
    kpi_recent = con.execute("""
        SELECT * FROM v_kpi_daily
        ORDER BY date DESC, group_name
        LIMIT 10
    """).fetchdf()

    con.close()

    print("\n[DuckDB Status Report]")
    print("="*60)
    print(f"Version: {version}")
    print("\n[Database List]")
    print(db_list.to_string(index=False))
    print("\n[Key Settings]")
    print(settings.to_string(index=False))
    print("\n[Top 10 Tables by Size]")
    print(storage_info.to_string(index=False))
    print("\n[Top 10 Tables by Row Count]")
    print(table_counts_df.to_string(index=False))
    print("\n[Recent KPI Data]")
    print(kpi_recent.to_string(index=False))

if __name__ == "__main__":
    if run_transform():
        show_duckdb_status()
