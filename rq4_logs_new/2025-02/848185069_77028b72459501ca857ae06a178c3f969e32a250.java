package com.rljj.switchswitchcommon.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JsonConverter {

    private static final ObjectMapper mapper;

    static {
        mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .configure(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false)
                .registerModule(new JavaTimeModule());
    }

    private JsonConverter() {
        throw new RuntimeException("Construct not support");
    }

    public static ObjectMapper getInstance() {
        return mapper;
    }

    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert object to JSON", e);
        }
    }

    public static <T> T toObject(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JSON to object. json : " + json, e);
        }
    }

    public static <T> T toObject(String json, TypeReference<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JSON to object. json : " + json, e);
        }
    }

    public static <T> T convert(Object obj, Class<T> type) {
        try {
            return mapper.convertValue(obj, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JSON to object.", e);
        }
    }
}