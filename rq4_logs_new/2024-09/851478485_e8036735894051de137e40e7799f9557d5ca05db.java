package org.detective.services.email;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

    @Autowired
    private JavaMailSender emailSender;

    public void sendEmail(String to, String subject, String body) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom("test@gmail.com"); // 발신자 이메일
        message.setTo(to);
        message.setSubject(subject);
        message.setText(body);

        emailSender.send(message);
        System.out.println("이메일이 성공적으로 발송되었습니다!");
    }
}