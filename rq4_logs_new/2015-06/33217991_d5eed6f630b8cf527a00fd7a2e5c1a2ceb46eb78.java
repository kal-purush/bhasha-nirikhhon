package edu.mc2.sms.json.jackson;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.mc2.sms.jpa.entity.Attendance;

public class ArrayMapDeserializer extends JsonDeserializer<Map<Integer,Attendance>> {

    @Override
    public Map<Integer,Attendance> deserialize(JsonParser jp, DeserializationContext context)
            throws IOException {
        ObjectMapper mapper = (ObjectMapper) jp.getCodec();
        if (jp.getCurrentToken().equals(JsonToken.START_OBJECT)) {
            return mapper.readValue(jp, new TypeReference<HashMap<Integer,Attendance>>() {
            });
        } else {
            //consume this stream
            mapper.readTree(jp);
            return new HashMap<Integer,Attendance>();
        }
    }
}