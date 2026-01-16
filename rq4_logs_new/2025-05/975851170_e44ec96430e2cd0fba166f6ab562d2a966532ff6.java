package com.example.demo.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.dto.MatchRequestDto;
import com.example.demo.dto.MatchResponseDto;
import com.example.demo.security.CustomUserDetails;
import com.example.demo.service.MatchingService;

import lombok.RequiredArgsConstructor;


@RestController // 이 클래스가 REST API를 처리하는 컨트롤러임을 명시
@RequestMapping("/api/matching") // Common URL Prefix 설정
@RequiredArgsConstructor // final field 자동 주입
public class MatchingController {

    private final MatchingService matchingService;

    // 매칭 요청
    @PostMapping("/wait")
    public ResponseEntity<MatchResponseDto> requestMatching(
        @AuthenticationPrincipal CustomUserDetails userDetails,
        @RequestBody MatchRequestDto matchRequestDto
        ) {            
            Long userId = userDetails.getId();
            int count = matchRequestDto.getCount();
            Long roomId = matchingService.requestMatching(userId, count);

            System.out.println("[DEBUG] userDetails: " + userDetails);
            System.out.println("[DEBUG] Access Token 인증된 사용자 ID: " + (userDetails != null ? userDetails.getId() : "null"));

            if (roomId != null) {
                return ResponseEntity.ok(
                    MatchResponseDto.builder()
                        .roomId(roomId)
                        .status("matched")
                        .build()
                );
            } else {
                return ResponseEntity.ok(
                    MatchResponseDto.builder()
                        .roomId(null)
                        .status("waiting")
                        .build()
                );
            }
    }
}