package com.boozallen.aissemble.alerting.core;

/*-
 * #%L
 * aiSSEMBLE::Foundation::Alerting::Core
 * %%
 * Copyright (C) 2021 Booz Allen
 * %%
 * This software package is licensed under the Booz Allen Public License. All Rights Reserved.
 * #L%
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * AlertDeserializer for use in messaging
 *
 */

public class AlertDeserializer implements Deserializer<Alert> {
    private static final Logger logger = LoggerFactory.getLogger(AlertDeserializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Alert deserialize(String topic, byte[] alert) {
        try {
            if (alert == null){
                logger.warn("No Alert received for deserialization");
                return null;
            }
            return objectMapper.readValue(new String(alert, StandardCharsets.UTF_8), Alert.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Alert");
        }
    }
}