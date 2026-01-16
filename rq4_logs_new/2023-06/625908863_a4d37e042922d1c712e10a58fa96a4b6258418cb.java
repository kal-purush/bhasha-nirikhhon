package com.brainstrom.meokjang.review.controller;

import com.brainstrom.meokjang.common.dto.response.ApiResponse;
import com.brainstrom.meokjang.review.dto.request.ReviewRequest;
import com.brainstrom.meokjang.review.service.ReviewService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

@RestController
public class ReviewController {

    private final ReviewService reviewService;

    @Autowired
    public ReviewController(ReviewService reviewService) {
        this.reviewService = reviewService;
    }

    @GetMapping("/review/{userId}")
    public ResponseEntity<ApiResponse> getReviewList(@PathVariable Long userId) {

        try {
            ApiResponse res = new ApiResponse(200, "리뷰 목록 조회 성공", reviewService.getReviewList(userId));
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "리뷰 목록 조회 실패", e.getMessage()));
        }
    }

    @PostMapping("/review/{dealId}")
    public ResponseEntity<ApiResponse> createReview(@PathVariable Long dealId, @RequestBody ReviewRequest reviewRequest, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "리뷰 작성 실패", result.getAllErrors()));
        }
        try {
            reviewService.save(dealId, reviewRequest);
            ApiResponse res = new ApiResponse(200, "리뷰 작성 성공", null);
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "리뷰 작성 실패", e.getMessage()));
        }
    }
}