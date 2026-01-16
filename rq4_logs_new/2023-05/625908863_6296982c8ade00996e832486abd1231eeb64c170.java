package com.brainstrom.meokjang.deal.controller;

import com.brainstrom.meokjang.common.dto.response.ApiResponse;
import com.brainstrom.meokjang.deal.dto.request.DealRequest;
import com.brainstrom.meokjang.deal.dto.response.DealInfoResponse;
import com.brainstrom.meokjang.deal.service.DealService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

@RestController
public class DealController {

    private final DealService dealService;

    @Autowired
    public DealController(DealService dealService) {
        this.dealService = dealService;
    }

    @GetMapping("/deal/{userId}/nearby")
    public ResponseEntity<ApiResponse> getDealList(@PathVariable Long userId, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 목록 조회 실패", result.getAllErrors()));
        }
        try {
            ApiResponse res = new ApiResponse(200, "거래 목록 조회 성공", dealService.aroundDealList(userId));
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 목록 조회 실패", e.getMessage()));
        }
    }

    @PostMapping("/deal")
    public ResponseEntity<ApiResponse> createDeal(@RequestBody DealRequest dealRequest, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 생성 실패", result.getAllErrors()));
        }
        try {
            dealService.save(dealRequest);
            ApiResponse res = new ApiResponse(200, "거래 생성 성공", null);
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 생성 실패", e.getMessage()));
        }
    }

    @GetMapping("/deal/{dealId}")
    public ResponseEntity<ApiResponse> getDealInfo(@PathVariable Long dealId, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 정보 조회 실패", result.getAllErrors()));
        }
        try {
            DealInfoResponse dealInfoResponse = dealService.getDealInfo(dealId);
            ApiResponse res = new ApiResponse(200, "거래 정보 조회 성공", dealInfoResponse);
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 정보 조회 실패", e.getMessage()));
        }
    }

    @PutMapping("/deal/{dealId}")
    public ResponseEntity<ApiResponse> updateDealInfo(@PathVariable Long dealId, @RequestBody DealRequest dealRequest, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 정보 수정 실패", result.getAllErrors()));
        }
        try {
            dealService.updateDealInfo(dealId, dealRequest);
            ApiResponse res = new ApiResponse(200, "거래 정보 수정 성공", null);
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 정보 수정 실패", e.getMessage()));
        }
    }

    @DeleteMapping("/deal/{dealId}")
    public ResponseEntity<ApiResponse> deleteDeal(@PathVariable Long dealId, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 삭제 실패", result.getAllErrors()));
        }
        try {
            dealService.deleteDeal(dealId);
            ApiResponse res = new ApiResponse(200, "거래 삭제 성공", null);
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "거래 삭제 실패", e.getMessage()));
        }
    }
}