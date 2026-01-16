package org.example;

import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;

import javax.jms.*;

public final class Jms {

    private Jms() {
    }

    public static void sendBytesMessage(Connection connection, byte[] data) throws JMSException {
        final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final var queue = session.createQueue("TEST");
        final var message = session.createBytesMessage();
        message.writeBytes(data);
        final var producer = session.createProducer(queue);
        producer.send(message);
    }

    public static void sendTextMessage(Connection connection, String data) throws JMSException {
        final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final var queue = session.createQueue("TEST");
        final var message = session.createTextMessage();
        message.setText(data);
        final var producer = session.createProducer(queue);
        producer.send(message);
    }

    public static Message receive(Connection connection) throws JMSException {
        final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final var queue = session.createQueue("TEST");
        final var consumer = session.createConsumer(queue);
        return consumer.receive(3000);
    }

    public static ActiveMQJMSConnectionFactory connectionFactory() {
        final var connectionFactory = new ActiveMQJMSConnectionFactory("tcp://localhost:61616");
        connectionFactory.setUser("amq");
        connectionFactory.setPassword("ENC(OVbgLuiCqsNxYMiX9Tda7iPSQV49k25nJWiVb+Bc66c=)");
        return connectionFactory;
    }

    public static ActiveMQJMSConnectionFactory connectionFactory(int minimalLargeMessageSize, boolean compressLargeMessage) {
        final var connectionFactory = connectionFactory();
        connectionFactory.setCompressLargeMessage(compressLargeMessage);
        connectionFactory.setMinLargeMessageSize(minimalLargeMessageSize);
        return connectionFactory;
    }
}