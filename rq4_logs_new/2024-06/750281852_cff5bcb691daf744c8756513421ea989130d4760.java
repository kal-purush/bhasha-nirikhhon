package advanced;


import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AddingANewKeyToExisting {

    @Test
    public void quickEditToJsonObject() throws JsonMappingException, JsonProcessingException {
        String jsonObject = "{\r\n" +
                "  \"location\": {\r\n" +
                "    \"lat\": -38.383494,\r\n" +
                "    \"lng\": 33.427362\r\n" +
                "  },\r\n" +
                "  \"accuracy\": 50,\r\n" +
                "  \"name\": \"Frontline house\",\r\n" +
                "  \"phone_number\": \"(+91) 983 893 3937\",\r\n" +
                "  \"address\": \"29, side layout, cohen 09\",\r\n" +
                "  \"types\": [\r\n" +
                "    \"shoe park\",\r\n" +
                "    \"shop\"\r\n" +
                "  ],\r\n" +
                "  \"website\": \"http://google.com\",\r\n" +
                "  \"language\": \"French-IN\"\r\n" +
                "}";

        // Creating an instance of ObjectMapper class
        ObjectMapper objectMapper = new ObjectMapper();
        // Get ObjectNode representation of json as json is an Object
        ObjectNode objectNode = objectMapper.readValue(jsonObject, ObjectNode.class);
        
        // Add a new field to the JSON object
        objectNode.put("newField", "newValue");
        
        System.out.println(objectNode.toPrettyString());

    }

   
}