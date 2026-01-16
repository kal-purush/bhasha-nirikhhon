package com.adidas.emailservice.messaging;

import com.adidas.emailservice.service.EmailDispatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class MessagingServiceStartup {

    String topic = "email";
    String broker = "tcp://127.0.0.1:1883";

    SubscriptionMessagingService messageService = new SubscriptionMessagingService(topic, broker);

    @EventListener(ApplicationReadyEvent.class)
    public void runAfterStartup() {
        messageService.subscribe();
    }
}