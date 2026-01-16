package com.everycare.backend.domain.chatbot.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "ChatGPT API", description = "챗GPT API - 일반 챗봇 / 복용 내역 모니터링")
@RequestMapping("/api/v1/chatbot")
@RequiredArgsConstructor
public class ChatGPTController {


//    @PostMapping("/ask")
//    @Operation(summary = "일반 채팅 API", description = "챗봇에게 질문할 때 사용")
//    public ResponseEntity<RestApiResponse askToChatGPT(@RequestBody ) {
//    }

//    @GetMapping("/monitoring")
//    @Operation(summary = "복용 내역 모니터링 API", description = "복용 내역 모니터링 버튼 클릭시 챗봇이 통계내주는 응답.")
//    public ResponseEntity<RestApiResponse askToChatGPT(@RequestBody ) {
//    }

}