package com.ecommerce.orderservice.services;

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
import com.ecommerce.orderservice.enums.OrderStatus;

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

    private void sendEmail(Mail mail, String recipientEmail, String emailType) {
        Request request = new Request();
        request.setMethod(Method.POST);
        request.setEndpoint("mail/send");

        try {
            request.setBody(mail.build());
            Response response = sendGrid.api(request);
            if (response.getStatusCode() != 202) {
                throw new RuntimeException("Failed to send email. Status code: " + response.getStatusCode());
            }
            
            log.info("{} email sent successfully to: {}", emailType, recipientEmail);
        } catch (IOException e) {
            throw new RuntimeException("Failed to send email", e);
        }
    }

    @Override
    public void sendOrderConfirmationEmail(String to, String orderId, double totalAmount) {
        Email from = new Email(fromEmail);
        Email toEmail = new Email(to);
        String subject = "Order Confirmation";
        String orderLink = frontendUrl + "/orders/" + orderId;
        String contentText = "Thank you for your order! Your order has been confirmed.\n\n" +
                "Order ID: " + orderId + "\n" +
                "Total Amount: $" + String.format("%.2f", totalAmount) + "\n\n" +
                "You can view your order details here: " + orderLink + "\n\n" +
                "If you have any questions, please contact our support team.";
        Content content = new Content("text/plain", contentText);
        Mail mail = new Mail(from, subject, toEmail, content);
        
        sendEmail(mail, toEmail.getEmail(), "Order confirmation");
    }

    @Override
    public void sendPaymentFailedEmail(String to, String orderId, OrderStatus status, String paymentLink) {
        Email from = new Email(fromEmail);
        Email toEmail = new Email(to);
        String subject = "Payment Failed";
        String orderLink = frontendUrl + "/orders/" + orderId;
        String contentText = "Your payment has failed. Please try again.\n\n" +
                "Order ID: " + orderId + "\n" +
                "New Status: " + status + "\n\n" +
                "You can view your order details here: " + orderLink + "\n\n" +
                "You can retry payment here: " + paymentLink + "\n\n" +
                "If you have any questions, please contact our support team.";
        Content content = new Content("text/plain", contentText);
        Mail mail = new Mail(from, subject, toEmail, content);        
        sendEmail(mail, toEmail.getEmail(), "Payment failed");
    }

    @Override
    public void sendOrderProcessingEmail(String to, String orderId, OrderStatus status, String paymentLink) {
        Email from = new Email(fromEmail);
        Email toEmail = new Email(to);
        String subject = "Order Processing";
        String orderLink = frontendUrl + "/orders/" + orderId;
        String contentText = "Your order is being processed and payment is successful. Please wait for it to be shipped.\n\n" +
                "Order ID: " + orderId + "\n" +
                "New Status: " + status + "\n\n" +
                "You can view your order details here: " + orderLink + "\n\n" +
                "You can view your payment receipt here: " + paymentLink + "\n\n" +
                "If you have any questions, please contact our support team.";
        Content content = new Content("text/plain", contentText);
        Mail mail = new Mail(from, subject, toEmail, content);
        sendEmail(mail, toEmail.getEmail(), "Order processing");
    }

} 