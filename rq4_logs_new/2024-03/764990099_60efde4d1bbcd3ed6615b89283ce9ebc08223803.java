package com.customs.network.fdapn.utils;

import com.customs.network.fdapn.model.CustomerDetails;
import com.customs.network.fdapn.model.ValidationError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static JsonNode convertCustomerDetailsToJson(CustomerDetails customerDetails) {
        try {
            return objectMapper.valueToTree(customerDetails);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error converting CustomerDetails to JsonNode", e);
        }
    }
    public static CustomerDetails convertJsonNodeToCustomerDetails(JsonNode jsonNode) {
        try {
            return objectMapper.treeToValue(jsonNode, CustomerDetails.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error converting JsonNode to CustomerDetails", e);
        }
    }
    public static JsonNode convertValidationErrorListToJson(List<ValidationError> validationErrors) {
        try {
            return objectMapper.valueToTree(validationErrors);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error converting List<ValidationError> to JsonNode", e);
        }
    }
}