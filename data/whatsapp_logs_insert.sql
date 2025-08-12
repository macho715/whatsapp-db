
CREATE TABLE IF NOT EXISTS chat_logs (
    date_gst TEXT,
    group_name TEXT,
    summary TEXT,
    top_keywords TEXT,
    sla_breaches INTEGER,
    attachments TEXT,
    top_senders TEXT
);

INSERT INTO chat_logs (date_gst,group_name,summary,top_keywords,sla_breaches,attachments,top_senders) VALUES ('2025-08-10 20:00','[HVDC] Project Lightning','Jopetwil-62 East Harbor LOLO→Shuttle→15:00 RORO 크레인 오프로딩, Ferry 지연으로 오퍼레이터 도착 지연, Warm-up 후 700T Crane Offload 완료. Jopetwil-71 MW4 대기 중, Wardeh-1 AGOI ETA 12:00, Thuraya DAS Offload/Backload→Bunkering, Kawakeeb Jetty#5 Offload 진행.','[''Jopetwil-62'', ''Crane'', ''Ferry delay'', ''RORO'', ''Offload'']','2','[]','[''Sajid H Khan(Logistics)'', ''Roy Kim(Ops)'', ''Ramaju Das(Site)'']');
INSERT INTO chat_logs (date_gst,group_name,summary,top_keywords,sla_breaches,attachments,top_senders) VALUES ('2025-08-09 09:55','Jopetwil 71 Group','2025-05-14~08-09: Jopetwil-71 Aggregate/Dune Sand 운송. High Tide·만조로 오프로딩 중단 다수, CEP 검사·항만 대기, 장비 고장(Bow Thruster/Port Engine) 장기 수리. FW 공급 지연·품질 문제. 08/09 20MM Aggregate 하역 완료 후 09:55 AGI 출항.','High Tide,Aggregate,Engine failure,CEP,Fresh Water','5','','Jptw71 Henok(Captain),Sajid H Khan(Logistics),Khemlal-SCT Logistics(Ops)');