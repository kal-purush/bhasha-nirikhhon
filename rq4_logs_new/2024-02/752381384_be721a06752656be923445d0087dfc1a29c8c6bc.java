package org.example.inmemorybroker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultMessageBrokerTest {

    private DefaultMessageBroker defaultMessageBroker;

    @BeforeEach
    void setUp() {
        defaultMessageBroker = new DefaultMessageBroker();
    }

    @Test
    void testConsumeMessageFromEmptyTopic() {
        String topicName = "emptyTopic";

        List<String> messages = defaultMessageBroker.consumeMessage(topicName);

        assertTrue(messages.isEmpty(), "Should return an empty list for a non-existent topic");
    }

    @Test
    void testCreateTopicAndSendMessageAndPriorityCheck() {
        String topicName = "multiMessageTopic";
        String firstMessage = "First Message";
        String secondMessage = "Second Message";
        defaultMessageBroker.createTopic(topicName);

        defaultMessageBroker.sendMessage(topicName, firstMessage);
        defaultMessageBroker.sendMessage(topicName, secondMessage);

        List<String> messages = defaultMessageBroker.consumeMessage(topicName);

        assertEquals(2, messages.size(), "Should have two messages in the topic");
        assertEquals(firstMessage, messages.get(0), "The message should match the sent message");

    }
}