# whatsapp_automation.py
import os
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict
import re

# ================================
# 1. DB 저장 함수
# ================================
def save_chat_log(group_name, df, period):
    save_dir = f"/data/whatsapp_logs"
    os.makedirs(save_dir, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")
    file_path = os.path.join(save_dir, f"{group_name.replace(' ', '_')}_{today_str}_{period}.sqlite")
    
    # 리스트형 컬럼을 문자열로 변환
    for col in df.columns:
        df[col] = df[col].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    
    conn = sqlite3.connect(file_path)
    df.to_sql("chat_log", conn, if_exists="append", index=False)
    conn.close()
    print(f"✅ 채팅 로그 저장 완료: {file_path}")

# ================================
# 2. 아침 7시 회의 보고서 생성
# ================================
def generate_morning_briefing(group_name, df):
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    report_dir = f"/data/reports/morning"
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, f"{group_name.replace(' ', '_')}_{today}.md")
    
    urgent = df[df['tags'].str.contains("URGENT", na=False)]
    action = df[df['tags'].str.contains("ACTION", na=False)]
    
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"# 📅 {today} 아침 회의 보고서 ({group_name})\n\n")
        f.write("## 1. 전날 주요 업무\n")
        f.write("\n".join(df[df['date'] == str(yesterday)]['message'].tolist()) + "\n\n")
        
        f.write("## 2. 오늘 할 일\n")
        f.write("\n".join(action['message'].tolist()) + "\n\n")
        
        f.write("## 3. 중요 이슈\n")
        f.write("\n".join(df[df['tags'].str.contains("IMPORTANT", na=False)]['message'].tolist()) + "\n\n")
        
        f.write("## 4. 급한 일 (URGENT)\n")
        f.write("\n".join(urgent['message'].tolist()) + "\n\n")
        
        f.write("## 5. 놓친 일 / 미처리 건\n")
        missed = df[df['sla_breach'] == 1]['message'].tolist()
        f.write("\n".join(missed) + "\n")
    
    print(f"✅ 아침 회의 보고서 생성 완료: {report_file}")

# ================================
# 3. 주간 보고서
# ================================
def generate_weekly_report(group_name, df):
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    
    weekly_df = df[(pd.to_datetime(df['date']) >= pd.to_datetime(week_start)) &
                   (pd.to_datetime(df['date']) <= pd.to_datetime(week_end))]
    
    total_messages = len(weekly_df)
    urgent_count = weekly_df['tags'].str.contains("URGENT", na=False).sum()
    sla_breach_count = weekly_df['sla_breach'].sum()
    
    top_keywords = ", ".join(weekly_df['message'].str.split(expand=True).stack().value_counts().head(5).index)
    
    report_dir = f"/data/reports/weekly"
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, f"{group_name.replace(' ', '_')}_{week_start}_weekly.md")
    
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"# 📊 {group_name} 주간 보고서 ({week_start} ~ {week_end})\n\n")
        f.write(f"- 총 메시지 수: {total_messages}\n")
        f.write(f"- URGENT 건수: {urgent_count}\n")
        f.write(f"- SLA 위반 건수: {sla_breach_count}\n")
        f.write(f"- 상위 키워드: {top_keywords}\n\n")
    
    print(f"✅ 주간 보고서 생성 완료: {report_file}")

# ================================
# 4. 월간 보고서
# ================================
def generate_monthly_report(group_name, df):
    today = datetime.now().date()
    month_start = today.replace(day=1)
    month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    monthly_df = df[(pd.to_datetime(df['date']) >= pd.to_datetime(month_start)) &
                    (pd.to_datetime(df['date']) <= pd.to_datetime(month_end))]
    
    total_messages = len(monthly_df)
    urgent_count = monthly_df['tags'].str.contains("URGENT", na=False).sum()
    sla_breach_count = monthly_df['sla_breach'].sum()
    
    top_keywords = ", ".join(monthly_df['message'].str.split(expand=True).stack().value_counts().head(5).index)
    
    report_dir = f"/data/reports/monthly"
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, f"{group_name.replace(' ', '_')}_{month_start.strftime('%Y%m')}_monthly.md")
    
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"# 📅 {group_name} 월간 보고서 ({month_start} ~ {month_end})\n\n")
        f.write(f"- 총 메시지 수: {total_messages}\n")
        f.write(f"- URGENT 건수: {urgent_count}\n")
        f.write(f"- SLA 위반 건수: {sla_breach_count}\n")
        f.write(f"- 상위 키워드: {top_keywords}\n\n")
    
    print(f"✅ 월간 보고서 생성 완료: {report_file}")

# ================================
# 5. 날짜 + 키워드 검색
# ================================
def search_chat(group_name, start_date, end_date, keyword):
    db_path = f"/data/whatsapp_logs/{group_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}_daily.sqlite"
    if not os.path.exists(db_path):
        print("❌ 해당 그룹의 데이터베이스 파일이 없습니다.")
        return
    
    conn = sqlite3.connect(db_path)
    query = f"""
        SELECT date, time, sender, message, attachments
        FROM chat_log
        WHERE date BETWEEN ? AND ? AND message LIKE ?
    """
    results = pd.read_sql_query(query, conn, params=[start_date, end_date, f"%{keyword}%"])
    conn.close()
    
    print(f"🔍 검색 결과 ({len(results)}건):\n", results)
    return results

# ================================
# 실행 예시
# ================================
if __name__ == "__main__":
    # 예시 DataFrame
    df_example = pd.DataFrame([
        {"date": "2025-08-08", "time": "09:00", "sender": "정상욱", "sender_role": "물류 PM", 
         "message": "RORO 작업 18:00 재개 예정, ALS145T 예약 확인", "tags": "ACTION", "sla_breach": 0, "attachments": ""},
        {"date": "2025-08-08", "time": "10:00", "sender": "Haitham", "sender_role": "포트 OPS", 
         "message": "크레인 도착 지연, ETA 11:00", "tags": "URGENT", "sla_breach": 1, "attachments": ""}
    ])
    
    save_chat_log("UPC – Precast Transportation", df_example, "daily")
    generate_morning_briefing("UPC – Precast Transportation", df_example)
    generate_weekly_report("UPC – Precast Transportation", df_example)
    generate_monthly_report("UPC – Precast Transportation", df_example)
    search_chat("UPC – Precast Transportation", "2025-08-07", "2025-08-09", "크레인")
