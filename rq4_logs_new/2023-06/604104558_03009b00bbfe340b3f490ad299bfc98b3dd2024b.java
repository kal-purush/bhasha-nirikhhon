package com.wee.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wee.service.PublishService;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.util.HashMap;


@Service
@RequiredArgsConstructor
public class EventsLogHelper {
    private  final ObjectMapper objectMapper;
    private final PublishService publishService;
    public void addAgentEvent(JSONObject metaData){
        HashMap<String,String> attributes = new HashMap<>();
       // String s =(String) metaData.getString("userId");
        attributes.put("userId", String.valueOf(metaData.get("userId")));
        attributes.put("type",metaData.getString("type"));
        attributes.put("referenceId", String.valueOf(metaData.get("referenceId")));
        try {
            generateEventPayload(attributes);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void generateEventPayload(HashMap<String, String> attributes) throws JsonProcessingException {
        String payload = objectMapper.writeValueAsString(attributes);
        publishService.logEvents(payload);
    }
}