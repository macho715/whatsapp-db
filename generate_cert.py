#!/usr/bin/env python3
"""
로컬 개발용 SSL 인증서 생성 스크립트 (localhost 전용)
"""
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta, timezone
import ipaddress

def generate_self_signed_cert():
    """localhost용 자체 서명 인증서 생성"""
    
    # RSA 키 생성
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    
    # 인증서 정보 (localhost 전용)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "KR"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Seoul"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "Seoul"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "HVDC Development"),
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    
    # 인증서 생성
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.now(timezone.utc)
    ).not_valid_after(
        datetime.now(timezone.utc) + timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            x509.IPAddress(ipaddress.IPv6Address("::1")),
        ]),
        critical=False,
    ).sign(private_key, hashes.SHA256())
    
    # 파일로 저장
    with open("key.pem", "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    with open("cert.pem", "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    print("✅ localhost용 SSL 인증서 생성 완료:")
    print("  - key.pem (개인키)")
    print("  - cert.pem (인증서)")
    print("  - CN: localhost")
    print("  - SAN: localhost, 127.0.0.1, ::1")
    print("  - 사용법: uvicorn main:app --host 0.0.0.0 --port 8000 --ssl-keyfile=key.pem --ssl-certfile=cert.pem")

if __name__ == "__main__":
    try:
        generate_self_signed_cert()
    except ImportError:
        print("❌ cryptography 라이브러리가 필요합니다:")
        print("pip install cryptography")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
