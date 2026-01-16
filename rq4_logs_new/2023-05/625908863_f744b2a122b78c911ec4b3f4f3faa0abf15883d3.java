package com.brainstrom.meokjang.user.controller;

import com.brainstrom.meokjang.common.dto.response.ApiResponse;
import com.brainstrom.meokjang.user.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

@RestController
public class UserController {

    private final UserService userService;

    @Autowired
    public UserController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping("/users/{userId}")
    public ResponseEntity<ApiResponse> getUserInfo(@PathVariable Long userId, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "유저 정보 조회 실패", result.getAllErrors()));
        } // 이 부분 GlobalExceptionHandler로 빼기
        try {
            ApiResponse res = new ApiResponse(200, "유저 정보 조회 성공", userService.getUserInfo(userId));
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "유저 정보 조회 실패", e.getMessage()));
        }
    }

    @PutMapping("/users/{userId}")
    public ResponseEntity<ApiResponse> updateUserInfo(@PathVariable Long userId, @RequestBody String userName, BindingResult result) {

        if (result.hasErrors()) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "유저 정보 수정 실패", result.getAllErrors()));
        } // 이 부분 GlobalExceptionHandler로 빼기
        try {
            ApiResponse res = new ApiResponse(200, "유저 정보 수정 성공", userService.updateUserInfo(userId, userName));
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.badRequest().body(new ApiResponse(400, "유저 정보 수정 실패", e.getMessage()));
        }
    }
}