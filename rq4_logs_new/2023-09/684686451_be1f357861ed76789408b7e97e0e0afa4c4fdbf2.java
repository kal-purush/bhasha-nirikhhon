package ru.mart.Hero.component.constants;

public class AliasConstants {
    public static final String CONSUMED_MESSAGE_LOG = "Consumed message from kafka: {}";
    public static final String TRYING_SEND_KAFKA_LOG = "Trying to send {} to kafka";
    public static final String SENT_TO_KAFKA_LOG = "Message {} sent to kafka";

    private AliasConstants(){
        throw new IllegalStateException("Utility class");
    }
}