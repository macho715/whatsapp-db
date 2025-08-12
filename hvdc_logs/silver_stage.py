import duckdb, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_FILE = os.path.join(BASE_DIR, 'duckdb', 'hvdc.duckdb')

def transform_raw_to_sla():
    con = duckdb.connect(DUCKDB_FILE)
    con.execute('''
        CREATE OR REPLACE TABLE sla_log AS
        SELECT date_gst, group_name, sender, sender_role,
               message, tags, top_keywords, sla_breaches, attachments
        FROM raw_logs
        WHERE date_gst IS NOT NULL
    ''')
    con.close()
    print('[SILVER] raw_logs transformed to sla_log')

if __name__ == '__main__':
    transform_raw_to_sla()
