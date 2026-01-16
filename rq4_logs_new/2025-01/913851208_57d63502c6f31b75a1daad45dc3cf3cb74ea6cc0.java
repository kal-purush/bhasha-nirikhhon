package com.project_merge.jigu_travel.api.auth.controller;

import com.project_merge.jigu_travel.api.auth.dto.LoginRequestDto;
import com.project_merge.jigu_travel.api.auth.dto.LoginResponseDto;
import com.project_merge.jigu_travel.api.auth.dto.RegisterRequestDto;
import com.project_merge.jigu_travel.api.auth.service.AuthService;
import com.project_merge.jigu_travel.exception.CustomException;
import com.project_merge.jigu_travel.global.common.BaseResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RequiredArgsConstructor
@RestController
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;
    @PostMapping("/login")
    public ResponseEntity<BaseResponse<LoginResponseDto>> login(@RequestBody LoginRequestDto loginRequest) {
        try {
            LoginResponseDto response = authService.login(loginRequest);
            return ResponseEntity.ok(new BaseResponse<>(200, "로그인 성공", response));
        } catch (CustomException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new BaseResponse<>(400, e.getMessage(), null));
        }
    }

    @PostMapping("/register")
    public ResponseEntity<BaseResponse<Void>> register(@RequestBody RegisterRequestDto registerRequest) {
        try {
            authService.register(registerRequest);
            return ResponseEntity.ok(new BaseResponse<>(200, "회원가입 성공", null));
        } catch (CustomException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new BaseResponse<>(400, e.getMessage(), null));
        }
    }

    @PostMapping("/logout")
    public ResponseEntity<?> logout(@RequestHeader("Authorization") String token) {
        try {
            if (token.startsWith("Bearer ")) {
                token = token.substring(7); // "Bearer " 제거
            }
            authService.logout(token);
            return ResponseEntity.ok(Map.of("message", "로그아웃 성공!"));
        } catch (CustomException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("message", e.getMessage()));
        }
    }

}