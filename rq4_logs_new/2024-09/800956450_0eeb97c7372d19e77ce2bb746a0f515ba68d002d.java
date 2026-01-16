package io.github.xpakx.images_integration.transformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.xpakx.images_integration.transformation.model.Account;
import io.github.xpakx.images_integration.transformation.model.Event;
import org.springframework.integration.transformer.AbstractPayloadTransformer;
import org.springframework.stereotype.Service;

@Service
public class StringToEventTransformer extends AbstractPayloadTransformer<String, Event<?>> {

    @Override
    protected Event<?> transformPayload(String event) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode value = objectMapper.readTree(event);
            JsonNode payloadNode = value.path("payload");

            var before = objectMapper
                    .treeToValue(payloadNode.path("before"), Account.class);
            var after = objectMapper
                    .treeToValue(payloadNode.path("after"), Account.class);
            return new Event<>(before, after, 0L, "account");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}