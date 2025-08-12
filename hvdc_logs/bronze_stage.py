import duckdb, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_FILE = os.path.join(BASE_DIR, 'duckdb', 'hvdc.duckdb')
INPUT_DIR = os.path.join(BASE_DIR, 'input')

def load_csv_to_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    con.execute(f'''
        CREATE OR REPLACE TABLE raw_logs AS
        SELECT * FROM read_csv_auto('{INPUT_DIR.replace("\\", "/")}/*.csv', ignore_errors=true)
    ''')
    con.close()
    print('[BRONZE] CSV loaded to raw_logs')

if __name__ == '__main__':
    load_csv_to_duckdb()
