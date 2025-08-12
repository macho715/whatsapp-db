import subprocess, os, duckdb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_FILE = os.path.join(BASE_DIR, 'duckdb', 'hvdc.duckdb')
TRANSFORM_SQL = os.path.join(BASE_DIR, 'transform.sql')

def run_pipeline_sequence():
    subprocess.run(['python', os.path.join(BASE_DIR, 'bronze_stage.py')], check=True)
    subprocess.run(['python', os.path.join(BASE_DIR, 'silver_stage.py')], check=True)

    con = duckdb.connect(DUCKDB_FILE)
    with open(TRANSFORM_SQL, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    con.execute(sql_script)
    con.close()
    print('[PIPELINE] Bronze → Silver → Transform completed')
    return {'status': 'ok'}

if __name__ == '__main__':
    run_pipeline_sequence()
