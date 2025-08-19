import PySimpleGUI as sg
import requests
import uuid
import datetime
import subprocess
import os
import sys
import json
import time
import hmac, hashlib, base64

# psutil은 ARM/Windows 환경에서 wheel 부재 시 설치가 어려울 수 있어 선택적으로 사용
try:
	import psutil  # type: ignore
	PSUTIL_AVAILABLE = True
except Exception:
	psutil = None  # type: ignore
	PSUTIL_AVAILABLE = False

# ===== 설정 =====
BASE_URL = os.getenv("HVDC_API_BASE", "http://127.0.0.1:8010")
API_KEY = os.getenv("API_KEY")
# venv 파이썬 보장 실행을 위해 현재 인터프리터 사용
UVICORN_CMD = [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8010", "--log-level", "info"]
UVICORN_PROCESS = None
LOG_FILE = "access_log.jsonl"

headers = {}
if API_KEY:
    headers["X-API-Key"] = API_KEY

# ===== uvicorn 프로세스 감지 =====
def detect_uvicorn():
	# psutil이 가능하면 기존 방식 사용
	if PSUTIL_AVAILABLE:
		for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
			try:
				cmdline_list = proc.info.get('cmdline')
				if not cmdline_list:
					continue
				cmdline = " ".join(cmdline_list).lower()
				if "uvicorn" in cmdline and "main:app" in cmdline:
					return proc.info['pid']
			except Exception:
				continue
		return None

	# PowerShell을 이용한 fallback (cmdline에 uvicorn과 main:app 포함 여부로 탐지)
	try:
		ps_cmd = (
			"Get-CimInstance Win32_Process | "
			"Where-Object { $_.CommandLine -and $_.CommandLine -match 'uvicorn' -and $_.CommandLine -match 'main:app' } | "
			"Select-Object -First 1 -ExpandProperty ProcessId"
		)
		out = subprocess.check_output(["powershell", "-NoProfile", "-Command", ps_cmd], stderr=subprocess.STDOUT, text=True)
		pid_str = out.strip()
		return int(pid_str) if pid_str else None
	except Exception:
		return None

# ===== API 기능 =====
def check_health():
    r = requests.get(f"{BASE_URL}/health", headers=headers, timeout=5)
    r.raise_for_status()
    return r.json()

def append_test_log():
    payload = {
        "date_gst": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "group_name": "[HVDC] Project Lightning",
        "summary": "GUI connection test log",
        "top_keywords": ["connection", "test"],
        "sla_breaches": 0,
        "attachments": [],
        "request_id": str(uuid.uuid4())
    }
    sig = None
    if os.getenv("HMAC_SECRET"):
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        mac = hmac.new(os.getenv("HMAC_SECRET").encode("utf-8"), raw, hashlib.sha256).digest()
        sig = base64.b64encode(mac).decode("ascii")
    req_headers = dict(headers)
    if sig:
        req_headers["X-Signature"] = sig
    r = requests.post(f"{BASE_URL}/logs", json=payload, headers=req_headers, timeout=10)
    r.raise_for_status()
    return r.json()

def get_kpi():
    r = requests.get(f"{BASE_URL}/kpi?since=2025-08-01&group_name=[HVDC] Project Lightning", headers=headers, timeout=5)
    r.raise_for_status()
    return r.json()

def run_transform():
    r = requests.post(f"{BASE_URL}/hvdc/transform", headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

# ===== 서버 제어 =====
def start_server():
    global UVICORN_PROCESS
    existing_pid = detect_uvicorn()
    if existing_pid:
        return f"⚠ 이미 실행 중 (PID {existing_pid})"
    UVICORN_PROCESS = subprocess.Popen(UVICORN_CMD, creationflags=subprocess.CREATE_NEW_CONSOLE)
    return f"✅ 서버 시작됨 (PID {UVICORN_PROCESS.pid})"

def stop_server():
	global UVICORN_PROCESS
	existing_pid = detect_uvicorn()
	if existing_pid:
		# psutil이 가능하면 우아하게 종료 시도
		if PSUTIL_AVAILABLE:
			try:
				psutil.Process(existing_pid).terminate()
				UVICORN_PROCESS = None
				return f"🛑 서버 종료됨 (PID {existing_pid})"
			except Exception as e:
				return f"❌ 종료 실패: {e}"
		# fallback: taskkill 강제 종료
		try:
			subprocess.check_output(["taskkill", "/PID", str(existing_pid), "/F"], stderr=subprocess.STDOUT)
			UVICORN_PROCESS = None
			return f"🛑 서버 종료됨 (PID {existing_pid})"
		except Exception as e:
			return f"❌ 종료 실패: {e}"
	return "⚠ 실행 중인 서버가 없습니다."

# ===== 로그 읽기 =====
def read_latest_logs(last_size):
    if not os.path.exists(LOG_FILE):
        return last_size, []
    current_size = os.path.getsize(LOG_FILE)
    if current_size == last_size:
        return last_size, []
    logs = []
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines()[-5:]:
            try:
                logs.append(json.loads(line.strip()))
            except:
                continue
    return current_size, logs

# ===== GUI =====
sg.theme("DarkBlue3")
layout = [
    [sg.Text("HVDC Local API Controller & Checker", font=("Arial", 14))],
    [sg.Multiline(size=(70, 18), key="-OUTPUT-", disabled=True)],
    [
        sg.Button("Server ON", size=(12, 1)),
        sg.Button("Server OFF", size=(12, 1)),
        sg.Button("Check Connection", size=(20, 1)),
        sg.Button("Detect Server", size=(15, 1))
    ],
    [sg.Button("Auto Run", size=(20, 1), button_color=("white", "green"))],
    [sg.Button("Exit", size=(10, 1))]
]

window = sg.Window("HVDC API Controller", layout, finalize=True)
last_log_size = 0

# ===== 메인 루프 =====
while True:
    event, values = window.read(timeout=5000)  # 5초마다 체크
    if event in (sg.WIN_CLOSED, "Exit"):
        if event is None:
            continue  # None 이벤트일 경우 창 유지
        break

    if event == "Server ON":
        msg = start_server()
        window["-OUTPUT-"].update(msg + "\n", append=True)

    elif event == "Server OFF":
        msg = stop_server()
        window["-OUTPUT-"].update(msg + "\n", append=True)

    elif event == "Detect Server":
        pid = detect_uvicorn()
        if pid:
            window["-OUTPUT-"].update(f"🔍 uvicorn 서버 실행 중 (PID {pid})\n", append=True)
        else:
            window["-OUTPUT-"].update("🔍 uvicorn 서버 실행 안 됨\n", append=True)

    elif event == "Check Connection":
        try:
            pid = detect_uvicorn()
            output = [f"🔍 현재 서버 PID: {pid if pid else '없음'}"]
            output.append(str(check_health()))
            output.append(str(append_test_log()))
            output.append(str(get_kpi()))
            window["-OUTPUT-"].update("\n\n".join(output) + "\n\n✅ Connection Test Completed.", append=False)
        except Exception as e:
            window["-OUTPUT-"].update(f"❌ Connection or API call failed:\n{e}", append=False)

    elif event == "Auto Run":
        try:
            output = []
            output.append(start_server())
            time.sleep(3)  # 서버 기동 대기
            output.append(f"Transform Result: {run_transform()}")
            output.append(f"KPI Result: {get_kpi()}")
            window["-OUTPUT-"].update("\n\n".join([str(o) for o in output]) + "\n\n✅ Auto Run Completed.", append=False)
        except Exception as e:
            window["-OUTPUT-"].update(f"❌ Auto Run failed:\n{e}", append=False)

    # === 실시간 로그 감시 ===
    last_log_size, new_logs = read_latest_logs(last_log_size)
    for log in new_logs:
        log_line = f"📥 {log['timestamp']} | {log['client_ip']} → {log['url']}"
        window["-OUTPUT-"].update(log_line + "\n", append=True)

window.close()
