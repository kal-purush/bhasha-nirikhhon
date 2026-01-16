package com.spring.mood.projectmvc.controller.restApiController;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/admin")
@RequiredArgsConstructor
public class AdminController {

    private Integer currentTopicId = 1; // 기본 값 설정 (필요에 따라 변경 가능)
    private final SimpMessagingTemplate messagingTemplate;

    @PostMapping("/updateTopic")
    public Map<String, Object> updateTopic(@RequestBody Map<String, Integer> request) {
        Integer newTopicId = request.get("topicId");
        System.out.println("newTopicId = " + newTopicId);

        // 새로운 Topic ID를 저장
        if (newTopicId != null) {
            currentTopicId = newTopicId; // 현재 토픽 ID 업데이트
            messagingTemplate.convertAndSend("/topic/currentTopic", Collections.singletonMap("topicId", newTopicId));
            System.out.println("Broadcasting new topicId: " + newTopicId);
        } else {
            throw new IllegalArgumentException("Topic ID is null");
        }

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        return response;
    }
    @GetMapping("/currentTopic")
    public Map<String, Object> getCurrentTopic() {
        Map<String, Object> response = new HashMap<>();
        response.put("topicId", currentTopicId);
        return response;
    }
}