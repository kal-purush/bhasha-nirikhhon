package com.example.backend.mail;

import com.example.backend.Exceptions.MailNotSentException;
import com.example.backend.messages.SuccessMessages;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;

import java.util.Map;

public class DefaultEmailService {

    @Autowired
    public JavaMailSender emailSender;

    @Autowired
    private SpringTemplateEngine thymeleafTemplateEngine;

    public String sendHtmlMessage(String to, String subject, String htmlBody) throws MailNotSentException {
        try {
            MimeMessage message = emailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(htmlBody, true);

            emailSender.send(message);
        } catch (MailException | MessagingException e) {
            throw new MailNotSentException(e);
        }

        return SuccessMessages.SEND_EMAIL_SUCCESS.getMessage();
    }

    public String sendMessageUsingThymeleafTemplate(String to, String subject, Map<String, Object> templateModel, String path) throws MailNotSentException {
        Context thymeleafContext = new Context();
        thymeleafContext.setVariables(templateModel);
        String htmlBody = thymeleafTemplateEngine.process(path, thymeleafContext);

        return sendHtmlMessage(to, subject, htmlBody);
    }
}