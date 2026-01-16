package io.github.xpakx.images_integration.config;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.messaging.MessageHeaders;
import org.springframework.integration.amqp.support.AmqpHeaderMapper;

public class CustomAmqpHeaderMapper implements AmqpHeaderMapper {
    @Override
    public MessageHeaders toHeadersFromReply(MessageProperties requestHeaders) {
        return null;
    }

    @Override
    public MessageHeaders toHeadersFromRequest(MessageProperties requestHeaders) {
        return null;
    }

    @Override
    public void fromHeadersToReply(MessageHeaders headers, MessageProperties replyHeaders) {
    }

    @Override
    public void fromHeadersToRequest(MessageHeaders headers, MessageProperties replyHeaders) {
        replyHeaders.setHeader("source", "debezium-cdc");
        replyHeaders.setHeader("version", "0.0.1");
    }
}