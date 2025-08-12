import requests
import uuid
from datetime import datetime, timedelta

# ===== 환경 설정 =====
API_URL = "http://127.0.0.1:8010/logs"  # main.py 실행 서버 주소
API_KEY = "dev-local-key"               # X-API-Key
headers = {"X-API-Key": API_KEY}

# ===== 전날 날짜 =====
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M")

# ===== 보고서 데이터 예시 =====
report_data = [
    {
        "date_gst": yesterday_str,
        "group_name": "[HVDC] Project Lightning",
        "summary": "다중 선박 SITREP 보고, CCU·FR·OT 재고 트래킹, OSDR 문서화",
        "top_keywords": ["offloading", "thuraya", "bushra", "razan", "wardeh"],
        "sla_breaches": 1,
        "attachments": []
    },
    {
        "date_gst": yesterday_str,
        "group_name": "Abu Dhabi Logistics",
        "summary": "DAS·AGI·MOSB·MW4 하역·게이트패스·장비 배차, HCS 반송·증빙, A-Frame 제한 적용",
        "top_keywords": ["vp24", "lda", "noted", "arrived", "delivery"],
        "sla_breaches": 3,
        "attachments": []
    }
]

# ===== API 업로드 =====
for row in report_data:
    payload = {
        "request_id": str(uuid.uuid4()),  # 멱등성 키
        **row
    }
    try:
        resp = requests.post(API_URL, json=payload, headers=headers)
        if resp.status_code == 200:
            print(f"[OK] {row['group_name']} 업로드 완료")
        else:
            print(f"[FAIL] {row['group_name']} 실패 - {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[ERROR] {row['group_name']} 업로드 예외: {e}")

print("=== 자동 API 업로드 완료 ===")
