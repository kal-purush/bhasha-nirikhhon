package io.github.xpakx.images_cdc.kafka;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xpakx.images_cdc.data.EventService;
import io.github.xpakx.images_cdc.debezium.model.Account;
import io.github.xpakx.images_cdc.debezium.model.Image;
import io.github.xpakx.images_cdc.debezium.model.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class KafkaCDCListener {
    public final EventService service;

    public KafkaCDCListener(EventService service) {
        this.service = service;
    }

    @KafkaListener(topics = "${kafka.account.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenAccount(String message) {
        System.out.println("Received account event: " + message);
        parseValue(message, Account.class)
                .ifPresent(service::saveUser);
    }

    @KafkaListener(topics = "${kafka.image.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenImages(String message) {
        System.out.println("Received image event: " + message);
    }

    private <T> Optional<Value<T>> parseValue(String value, Class<T> clazz) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode valueNode = objectMapper.readTree(value);
            String schemaName = valueNode.path("schema").path("name").asText();
            Class<?> type = schemaToClass(schemaName);
            if (type == null || type != clazz) {
                System.out.println("Wrong type");
                System.out.println("Expected: " + clazz.getName());
                System.out.println("Found: " + (type != null ? type.getName() : "null"));
                return Optional.empty();
            }

            JsonNode payloadNode = valueNode.path("payload");

            T before = objectMapper
                    .treeToValue(payloadNode.path("before"), clazz);
            T after = objectMapper
                    .treeToValue(payloadNode.path("after"), clazz);

            return Optional.of(new Value<>(before, after));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Error");
        return Optional.empty();
    }

    public Class<?> schemaToClass(String schemaName) {
        return switch (schemaName) {
            case "postgres.public.account.Envelope" -> Account.class;
            case "postgres.public.image.Envelope" -> Image.class;
            default -> null;
        };
    }
}