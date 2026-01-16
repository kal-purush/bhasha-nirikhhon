package com.project_merge.jigu_travel.api.ai_classification.controller;

import com.project_merge.jigu_travel.api.ai_classification.dto.RecommendationRequestDto;
import com.project_merge.jigu_travel.api.ai_classification.dto.RecommendationResponseDto.RecommendationData;
import com.project_merge.jigu_travel.api.ai_classification.service.UserInterestService;
import com.project_merge.jigu_travel.global.common.BaseResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/ai/ai_classification")
@RequiredArgsConstructor
public class UserInterestController {
    private final UserInterestService userInterestService;

    @PostMapping("/fetch")
    public ResponseEntity<BaseResponse<RecommendationData>> fetchAndSaveUserInterest(
            @RequestBody RecommendationRequestDto requestDto) {

        System.out.println("🔹 [DEBUG] 요청 받은 데이터: " + requestDto);

        try {
            RecommendationData response = userInterestService.fetchAndSaveUserInterest(requestDto);

            // ✅ `RecommendationData`만 감싸서 정상 응답 반환
            return ResponseEntity.ok(new BaseResponse<>(200, "SUCCESS", response));

        } catch (IllegalArgumentException e) {
            // 잘못된 입력값 예외 처리
            return ResponseEntity.badRequest().body(new BaseResponse<>(400, "잘못된 요청: " + e.getMessage(), null));

        } catch (RuntimeException e) {
            // 서버 내부 오류 처리
            return ResponseEntity.internalServerError().body(new BaseResponse<>(500, "서버 오류 발생: " + e.getMessage(), null));
        }
    }
}