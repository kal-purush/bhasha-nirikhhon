package com.j30n.stoblyx.adapter.in.web.controller;

import com.j30n.stoblyx.adapter.in.web.dto.auth.LoginRequest;
import com.j30n.stoblyx.adapter.in.web.dto.auth.SignUpRequest;
import com.j30n.stoblyx.adapter.in.web.dto.auth.TokenResponse;
import com.j30n.stoblyx.adapter.in.web.support.TokenExtractor;
import com.j30n.stoblyx.application.service.auth.AuthService;
import com.j30n.stoblyx.common.response.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 인증 관련 API를 처리하는 컨트롤러
 * 회원가입, 로그인, 토큰 갱신, 로그아웃 기능을 제공합니다.
 * 모든 엔드포인트는 API 버전 v1을 사용합니다.
 * JWT 토큰 기반의 인증을 사용하며, Access Token과 Refresh Token을 관리합니다.
 */
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);
    private final AuthService authService;
    private final TokenExtractor tokenExtractor;

    /**
     * 새로운 사용자를 등록합니다.
     * 이메일 중복 검사를 수행하며, 비밀번호는 암호화되어 저장됩니다.
     *
     * @param request 회원가입 요청 DTO (이메일, 비밀번호, 사용자명 등 포함)
     * @return 회원가입 결과
     */
    @PostMapping("/signup")
    public ResponseEntity<ApiResponse<?>> signUp(@Valid @RequestBody SignUpRequest request) {
        authService.signUp(request);
        return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "회원가입이 완료되었습니다.", null));
    }

    /**
     * 사용자 로그인을 처리합니다.
     * 인증 성공 시 Access Token과 Refresh Token을 발급합니다.
     *
     * @param request 로그인 요청 DTO (이메일, 비밀번호 포함)
     * @return 인증 토큰 정보 (Access Token, Refresh Token)
     */
    @PostMapping("/login")
    public ResponseEntity<ApiResponse<TokenResponse>> login(@Valid @RequestBody LoginRequest request) {
        TokenResponse tokenResponse = authService.login(request);
        return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "로그인이 완료되었습니다.", tokenResponse));
    }

    /**
     * Refresh Token을 사용하여 새로운 Access Token을 발급합니다.
     * Access Token이 만료된 경우 이 엔드포인트를 통해 새로운 토큰을 얻을 수 있습니다.
     *
     * @param bearerToken Authorization 헤더에 포함된 Refresh Token (Bearer 형식)
     * @return 새로 발급된 인증 토큰 정보
     */
    @PostMapping("/refresh")
    public ResponseEntity<ApiResponse<TokenResponse>> refresh(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        String refreshToken = tokenExtractor.extractToken(bearerToken);
        TokenResponse tokenResponse = authService.refreshToken(refreshToken);
        return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "토큰이 갱신되었습니다.", tokenResponse));
    }

    /**
     * 사용자 로그아웃을 처리합니다.
     * Access Token을 블랙리스트에 추가하여 더 이상 사용할 수 없게 만듭니다.
     *
     * @param bearerToken Authorization 헤더에 포함된 Access Token (Bearer 형식)
     * @return 로그아웃 처리 결과
     */
    @PostMapping("/logout")
    public ResponseEntity<ApiResponse<?>> logout(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        String accessToken = tokenExtractor.extractToken(bearerToken);
        authService.logout(accessToken);
        return ResponseEntity.ok(new ApiResponse<>("SUCCESS", "로그아웃이 완료되었습니다.", null));
    }
}