#!/usr/bin/env python
import pexpect
import shlex
import sys
import os

CERT_KEY_FILE = "newcert.pem"
PRIVATE_KEY_FILE = "newkey.nopass.key"
MERGE_KEY_FILE = "all.pem"
PKCS12_FILE = "server_cert.p12"
JKS_FILE = "server.jks"
PKCS12_PASSWD = "webiznet"
JKS_PASSWD = "kurento"

if os.path.exists(JKS_FILE):
    # 초기 동작은 파일이 존재하면 재성성을 하지 않는다
    print("JKS 파일이 존재합니다. 동작을 멈춥니다")
    sys.exit()

cert_key = open(CERT_KEY_FILE, "r").readlines()
private_key = open(PRIVATE_KEY_FILE, "r").read()

all_key = open(MERGE_KEY_FILE, "w")

checked_certificate_line = False

for line in cert_key:
    if line.startswith("-----BEGIN CERTIFICATE-----"):
        checked_certificate_line = True

    if checked_certificate_line:
        all_key.write(line)

all_key.writelines(private_key)
all_key.close()

# pem to pkcs12
pkcs12_cmd = "openssl pkcs12 -export -inkey {PRIVATE_KEY_FILE}" \
             " -in {MERGE_KEY_FILE} -name server_cert" \
             " -out {PKCS12_FILE}".format(**dict(
                PRIVATE_KEY_FILE=PRIVATE_KEY_FILE,
                MERGE_KEY_FILE=MERGE_KEY_FILE,
                PKCS12_FILE=PKCS12_FILE))

pkcs12_child = pexpect.spawn(pkcs12_cmd, echo=True)
pkcs12_child.expect("Enter Export Password:")
pkcs12_child.sendline(PKCS12_PASSWD)
pkcs12_child.expect("Verifying - Enter Export Password:")
pkcs12_child.sendline(PKCS12_PASSWD)
pkcs12_child.interact()

keytool_cmd = "keytool -importkeystore -srckeystore {PKCS12_FILE}" \
              " -srcstoretype pkcs12 -destkeystore {JKS_FILE}".format(**dict(
                 PKCS12_FILE=PKCS12_FILE,
                 JKS_FILE=JKS_FILE))

jks_child = pexpect.spawn(keytool_cmd, echo=True)
jks_child.expect("대상 키 저장소 비밀번호 입력: ".encode("utf-8"))
jks_child.sendline(JKS_PASSWD)
jks_child.expect("새 비밀번호 다시 입력:".encode("utf-8"))
jks_child.sendline(JKS_PASSWD)
jks_child.expect("소스 키 저장소 비밀번호 입력:".encode("utf-8"))
jks_child.sendline(PKCS12_PASSWD)
jks_child.interact()

for entry in [MERGE_KEY_FILE, PKCS12_FILE]:
    if os.path.exists(entry):
        os.remove(entry)