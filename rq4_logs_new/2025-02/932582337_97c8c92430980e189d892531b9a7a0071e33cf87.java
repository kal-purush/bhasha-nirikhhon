package com.j30n.stoblyx.adapter.web.controller;

import com.j30n.stoblyx.adapter.web.dto.auth.LoginRequest;
import com.j30n.stoblyx.adapter.web.dto.auth.SignUpRequest;
import com.j30n.stoblyx.adapter.web.dto.auth.TokenResponse;
import com.j30n.stoblyx.application.service.auth.AuthService;
import com.j30n.stoblyx.common.response.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/signup")
    public ResponseEntity<ApiResponse<?>> signUp(@Valid @RequestBody SignUpRequest request) {
        try {
            authService.signUp(request);
            return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "회원가입이 완료되었습니다.", null));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(new ApiResponse<>("ERROR", e.getMessage(), null));
        }
    }

    @PostMapping("/login")
    public ResponseEntity<ApiResponse<TokenResponse>> login(@Valid @RequestBody LoginRequest request) {
        try {
            TokenResponse tokenResponse = authService.login(request);
            return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "로그인이 완료되었습니다.", tokenResponse));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(new ApiResponse<>("ERROR", e.getMessage(), null));
        }
    }

    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<TokenResponse>> refresh(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        try {
            String refreshToken = bearerToken.substring(7);
            TokenResponse tokenResponse = authService.refreshToken(refreshToken);
            return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "토큰이 갱신되었습니다.", tokenResponse));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(new ApiResponse<>("ERROR", e.getMessage(), null));
        }
    }

    @PostMapping("/logout")
    public ResponseEntity<ApiResponse<?>> logout(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        try {
            String accessToken = bearerToken.substring(7);
            authService.logout(accessToken);
            return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "로그아웃이 완료되었습니다.", null));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(new ApiResponse<>("ERROR", e.getMessage(), null));
        }
    }
} 