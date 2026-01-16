package com.owiseman.embedding.service;

import com.owiseman.embedding.plugin.EmbeddingPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class CommandHandler {

    private static final Logger logger = LoggerFactory.getLogger(CommandHandler.class);

    @Autowired
    private EmbeddingPlugin embeddingPlugin;

    public Map<String, Object> handleCommand(String command, Map<String, String> parameters) {
        logger.info("Handling command: {} with parameters: {}", command, parameters);
        
        Map<String, Object> result = new HashMap<>();
        
        switch (command.toLowerCase()) {
            case "publish":
                return handlePublish(parameters);
//            case "status":
//                return handleStatus();
//            case "restart":
//                return handleRestart();
            default:
                result.put("success", false);
                result.put("error", "Unknown command: " + command);
                return result;
        }
    }

    private Map<String, Object> handlePublish(Map<String, String> parameters) {
        Map<String, Object> result = new HashMap<>();
        
        String topic = parameters.get("topic");
        String message = parameters.get("message");
        String qosStr = parameters.get("qos");
        
        if (topic == null || message == null) {
            result.put("success", false);
            result.put("error", "Missing required parameters: topic and message");
            return result;
        }
        
        int qos = 0;
        if (qosStr != null) {
            try {
                qos = Integer.parseInt(qosStr);
                if (qos < 0 || qos > 2) {
                    qos = 0;
                }
            } catch (NumberFormatException e) {
                // 使用默认值
            }
        }
        

        
        return result;
    }

}