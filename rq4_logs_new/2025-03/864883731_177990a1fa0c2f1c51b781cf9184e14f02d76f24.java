package com.ecommerce.usermanagementservice.services;

import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.Response;
import com.sendgrid.SendGrid;
import com.sendgrid.helpers.mail.Mail;
import com.sendgrid.helpers.mail.objects.Content;
import com.sendgrid.helpers.mail.objects.Email;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class SendGridEmailServiceImpl implements EmailService {
    
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SendGridEmailServiceImpl.class);

    @Autowired
    private SendGrid sendGrid;

    @Value("${sendgrid.from-email}")
    private String fromEmail;

    @Value("${app.frontend-url}")
    private String frontendUrl;

    @Override
    public void sendPasswordResetEmail(String to, String resetToken) {
        Email from = new Email(fromEmail);
        Email toEmail = new Email(to);
        String subject = "Password Reset Request";
        String resetLink = frontendUrl + "/reset-password?token=" + resetToken;
        String contentText = "Click the link below to reset your password. This link will expire in 1 hour.\n\n" +
                resetLink + "\n\n" +
                "If you didn't request this password reset, please ignore this email.";
        Content content = new Content("text/plain", contentText);
        Mail mail = new Mail(from, subject, toEmail, content);
        Request request = new Request();
        request.setMethod(Method.POST);
        request.setEndpoint("mail/send");

        try {
            request.setBody(mail.build());
            Response response = sendGrid.api(request);
            if (response.getStatusCode() != 202) {
                throw new RuntimeException("Failed to send email. Status code: " + response.getStatusCode());
            }
            
            log.info("Password reset email sent successfully to: {}", toEmail.getEmail());
        } catch (IOException e) {
            throw new RuntimeException("Failed to send email", e);
        }
    }
} 