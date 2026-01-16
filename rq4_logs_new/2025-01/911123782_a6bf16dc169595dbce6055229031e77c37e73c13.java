package com.roomfit.be.email;

import jakarta.mail.MessagingException;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * TODO: 추후 Retry 로직 추가 필요
 */
@Component
@RequiredArgsConstructor
public class EmailEventListener {
    private final EmailService emailService;
    @EventListener
    @Async("taskExecutor")
    public void onEmailSendEventHandler(EmailSendEvent event) throws MessagingException {
        emailService.sendEmail(event.recipient(), event.subject(), event.body());
    }
}