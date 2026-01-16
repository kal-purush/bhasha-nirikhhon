#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

domain = "smtp.dooray.com"
port = 465
email_address = "admin@naverunion.com"
password = "spdlql@0402"

mail = smtplib.SMTP_SSL(domain, port)
mail.login(email_address, password)


def send(to_mail, subject, contents):
    msg = MIMEMultipart("alternative")
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = email_address
    msg['To'] = to_mail
    msg.attach(MIMEText(contents, 'html', 'utf-8'))
    mail.sendmail(email_address, to_mail, msg.as_string())


if __name__ == "__main__":
    data = """
1	홍길	010-1111-1111	그린팩토리	NAVER Corporation	mail@navercorp.com	http://118.67.132.225:10080/?code=코드
    """

    title = "[공동성명] 2020년 귀속 연말정산용 기부금 영수증 발급"
    contents = """
<p>안녕하세요, 공동성명입니다.</p>
<br/>
<p>&lt;{name}&gt; 님의 2020년 기부금 영수증 발급을 안내드립니다.</p>
<p>2020년 기부금 영수증은 아래 URL로 접속하셔서 로그인 후 직접 출력하실 수 있습니다.</p>
<br/>
<p> * 영수증 발급받기 : <a href="{url}">이동하기</a></p>
<p> * 개인코드 : {code}</p>
<br/>
<p>기부금 영수증 발급에 필요한 정보(주민등록번호, 개인 주소)는 서버에 저장되지 않으며, 영수증 출력(파일 저장 또는 인쇄) 시에만 활용됩니다.</p>
<p>소득세법 34조에 따라 기부금 영수증에 주민등록번호는 필수로 기재되어야 하니, 이 점 참고하세요. (수기 입력도 가능)</p>
<br/>
<p>기타 문의 사항은 박상희 사무장에게 사내 이메일, 웍스 메신저, 카카오 플러스친구 등의 채널을 통해 문의주세요.</p>
<p>감사합니다.</p>
            """.strip()
    import time
    count = 0
    for rows in data.split("\n"):
        count += 1
        if len(rows.strip()) == 0:
            continue
        token = rows.split("\t")
        code = token[6].strip().split("?code=")[1]
        args = dict(seq=token[0].strip(), name=token[1].strip(), email=token[5].strip(), url=token[6].strip(), code=code)
        print("{seq}\t{name}\t{email}\t{url}".format(**args))
        send(args["email"], subject=title, contents=contents.format(**args))
        if count % 50 == 0:
            mail.close()
            mail = smtplib.SMTP_SSL(domain, port)
            mail.login(email_address, password)
    mail.close()
    print("성공")





