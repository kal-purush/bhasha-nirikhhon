package com.jgp.notification.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmailServiceImpl implements EmailService{

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;

    @Value("${jgp.notification.enabled}")
    private boolean notificationEnabled;

    @Value("${spring.mail.username}")
    private String notificationSenderEmail;

    @Override
    public void sendEmailNotification(String to, String subject, String content) {
    if (!notificationEnabled) {
        return;
    }
    MimeMessage message = mailSender.createMimeMessage();
    MimeMessageHelper helper = null; // true = multipart
    try {
        helper = new MimeMessageHelper(message, true);
        helper.setTo(to);
        helper.setSubject(subject);
        helper.setText(content, true); // true = HTML

        helper.setFrom(notificationSenderEmail);

        mailSender.send(message);
    } catch (MessagingException e) {
        log.error("Error sending email: {}", e.getMessage(), e);
    }
    }

    @Override
    public void sendEmailNotificationForDataReview(String toEmail, String toName, String dataType) {
        if (!notificationEnabled) {
            return;
        }
        Context context = new Context();
        context.setVariable("name", toName);
        context.setVariable("dataType", dataType);
        String htmlContent = templateEngine.process("email-template", context);

        MimeMessage message = mailSender.createMimeMessage();
        MimeMessageHelper helper = null; // true = multipart
        try {
            helper = new MimeMessageHelper(message, true);
            helper.setTo(toEmail);
            helper.setSubject("Data Review");
            helper.setText(htmlContent, true); // true = HTML

            helper.setFrom(notificationSenderEmail);

            mailSender.send(message);
        } catch (MessagingException e) {
            log.error("Error sending email: {}", e.getMessage(), e);
        }
    }
}