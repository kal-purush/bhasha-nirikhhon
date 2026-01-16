package no.fint.relations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FintResourceCompatibility {

    @Autowired
    private ObjectMapper objectMapper;

    public boolean isFintResourceData(List<?> data) {
        JsonNode node = objectMapper.valueToTree(data.get(0));
        return node.hasNonNull("resource");
    }
}