package com.roomfit.be.email.application;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;


@Getter
public final class EmailSendEvent extends ApplicationEvent {
    private final String recipient;
    private final String subject;
    private final String body;
    private EmailSendEvent(Object source, String recipient, String subject, String body) {
        super(source);
        this.recipient = recipient;
        this.subject = subject;
        this.body = body;
    }
    public static EmailSendEvent of(Object source, String recipient, String subject, String body){
        return new EmailSendEvent(source, recipient, subject, body);
    }

}
