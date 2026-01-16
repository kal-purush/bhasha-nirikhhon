package com.mydiary.my_diary_server.controller;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
@RequestMapping("/api")
public class TimeApiController {

    @GetMapping("/korean-time")
    @Operation(summary = "현재 시간 반환")
    public String getKoreanTime() {
        // 현재 한국 시간 가져오기
        LocalDateTime koreanTime = LocalDateTime.now().plusHours(0); // UTC+9 (한국 표준시)

        System.out.println("korea time: " + koreanTime);

        // 시간 형식 지정 (예: "yyyy-MM-dd HH:mm:ss")
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 형식에 맞게 포맷팅하여 반환
        return koreanTime.format(formatter);
    }
}