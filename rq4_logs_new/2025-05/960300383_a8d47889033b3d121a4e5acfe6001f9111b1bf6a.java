package com.github.codeboyzhou.mcp.declarative.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.codeboyzhou.mcp.declarative.exception.McpServerException;

public final class JsonHelper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new McpServerException("Error converting object to JSON", e);
        }
    }

}