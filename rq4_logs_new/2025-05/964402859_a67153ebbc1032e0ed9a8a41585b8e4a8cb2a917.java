package com.fatepet.petrest;

import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MailService {

    private final JavaMailSender mailSender;
    private static final String SUBJECT = "페이트 펫 상담 요청 알림";

    public void sendMail(String businessEmail, String customerPhoneNumber, String contactTypeName, String inquiry) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, false, "UTF-8");

            helper.setTo(businessEmail);
            helper.setSubject(SUBJECT);
            helper.setText(buildEmailBody(customerPhoneNumber, contactTypeName, inquiry), true); // true = HTML 형식
            mailSender.send(message);
            log.info("상담 요청 메일 전송 성공");
        } catch (Exception e) {
            log.error("상담 요청 메일 전송 실패", e);
        }
    }

    private String buildEmailBody(String customerPhoneNumber, String contactTypeName, String inquiry) {
        return String.format("""
            <div>
                <p><strong>페이트 펫을 통한 상담 요청이 있습니다.</strong></p>
                <p>현장결제단계에서 최종 금액의 <strong>5%% 할인</strong>을 적용 부탁드립니다.</p>
                <br/>
                <p><strong>연락처:</strong> %s</p>
                <p><strong>연락 방법:</strong> %s</p>
                <p><strong>기타 문의 사항:</strong> %s</p>
            </div>
            """, customerPhoneNumber, contactTypeName, (inquiry == null || inquiry.isBlank()) ? "없음" : inquiry);
    }
}