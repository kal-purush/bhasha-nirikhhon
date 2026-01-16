package org.frank.java.jackson.util.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.sun.istack.internal.NotNull;

import com.sun.istack.internal.Nullable;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class JsonUtil {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    
    public JsonUtil(){}
    
    @NotNull
    public static String toJsonString(Object data){
        if (ObjectUtils.isEmpty(data)){
            return "";
        }
        ObjectMapper objectMapper = getObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);       

        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {            
            logger.error("JsonUtil.toJsonString error", e);
            return "";
        }
    }
    
    
    @Nullable    
    public static<T> T parseObject(String jsonStr, TypeReference<T> valueTypeRef){
        if (ObjectUtils.isNotEmpty(jsonStr)){
            ObjectMapper objectMapper = getObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            try {
                return objectMapper.readValue(jsonStr, valueTypeRef);
            } catch (IOException e) {
                logger.error("JSONUtil.parseObject.TypeReference.error", e);
                return null;
            }
        }
        return null;
    }
    
    @Nullable    
    public static<T> T parseObject(String jsonStr, Class<T> clazz){
        if (ObjectUtils.isEmpty(jsonStr)){
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            return objectMapper.readValue(jsonStr, clazz);
        } catch (IOException e) {
            logger.error("JSONUtil.parseObject.error", e);
            return null;
        }
    }

    @Nullable
    public static <T> T parseSnakeObject(String json, Class<T> clazz) {
        if(ObjectUtils.isEmpty(json)){
            return null;
        }
        ObjectMapper objectMapper = getObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            logger.error("JSONUtil.parseSnakeObject.error", e);
            return null;
        }
    }
    
    private static ObjectMapper getObjectMapper(){
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        objectMapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
        return objectMapper;
    }
    
    
}