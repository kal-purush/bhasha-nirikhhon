package com.gowri.blitz.producers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/*
 * @author NaveenWodeyar
 * @date 23-03-2025
 */

@ExtendWith(MockitoExtension.class)
public class KafkaProducerServiceTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerServiceTest.class);

    @InjectMocks
    private KafkaProducerService kafkaProducerService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setUp() {
        // You can initialize any necessary components here if needed
    }

    @Test
    public void testSendMessage_shouldSendMessageToKafka() {
        // Arrange
        String message = "Test Message";

        // Act
        kafkaProducerService.sendMessage(message);

        // Assert
        verify(kafkaTemplate, times(1)).send("my_topic", message);  // Verify that send is called once with the correct parameters
        verify(logger, times(1)).info("Sending message: {}", message);  // Verify the info log for message
    }

    @Test
    public void testSendMessage_shouldSendMessageToKafkaWithCustomTopic() {
        // Arrange
        String customTopic = "custom_topic";
        String message = "Custom Topic Message";
        // Override the topic value in the KafkaProducerService instance
        kafkaProducerService.topic = customTopic;

        // Act
        kafkaProducerService.sendMessage(message);

        // Assert
        verify(kafkaTemplate, times(1)).send(customTopic, message);  // Verify that send is called with custom topic
        verify(logger, times(1)).info("Sending message: {}", message);  // Verify the info log for message
    }
}