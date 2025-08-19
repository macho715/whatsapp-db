import os
from pathlib import Path


def main() -> None:
	project_dir = Path(__file__).parent.resolve()
	os.chdir(project_dir)
	flag_file = project_dir / "fallback_sqlite.ON"

	ok = False
	try:
		import duckdb  # type: ignore
		db_path = project_dir / "hvdc_logs" / "duckdb" / "hvdc.duckdb"
		conn = duckdb.connect(str(db_path)) if db_path.exists() else duckdb.connect(":memory:")
		conn.close()
		ok = True
	except Exception:
		ok = False

	if ok:
		if flag_file.exists():
			flag_file.unlink()
		print("duckdb_health: OK")
	else:
		flag_file.write_text("auto\n", encoding="utf-8")
		print("duckdb_health: FAIL")


if __name__ == "__main__":
	main()


