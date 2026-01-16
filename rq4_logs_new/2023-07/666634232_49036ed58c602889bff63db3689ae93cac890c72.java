package groupone.userservice.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import groupone.userservice.entity.SimpleMessage;

public class SerializeUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static String serialize(SimpleMessage message){

        String result = null;

        try {
            result = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return result;
    }

}