package com.example.demo.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import lombok.RequiredArgsConstructor;

import com.example.demo.dto.AccessTokenResponseDto;
import com.example.demo.dto.AuthRequestDto;
import com.example.demo.dto.AuthResponseDto;
import com.example.demo.service.AuthService;
import com.example.demo.util.TokenUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import lombok.extern.slf4j.Slf4j;            // ← 추가


@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
@Slf4j   // <- 추가
public class AuthController {

    private final AuthService authService;

    /**
     * 로그인 후 accessToken과 refreshToken을 함께 반환합니다.
     */
    @PostMapping("/login")
    public ResponseEntity<AuthResponseDto> login(
            @RequestBody AuthRequestDto authRequestDto) {

        AuthResponseDto tokens = authService.login(authRequestDto);
        return ResponseEntity.ok(tokens);
    }

    // 로그아웃
    @PostMapping("/logout")
    public ResponseEntity<Void> logout(@RequestHeader("Authorization") String bearerToken) {
        
        // refresh token
        String refreshToken = TokenUtils.extractRefreshFrom(bearerToken);
        authService.logout(refreshToken); //  DB rfToken에서 삭제
        return ResponseEntity.noContent().build();
    }

    // refresh token으로 새로운 access token 재발급
    @PostMapping("/refresh")
    public ResponseEntity<AccessTokenResponseDto> refreshAccessToken(
            @RequestHeader(value = "Authorization", required = false) String authorizationHeader, 
            @CookieValue(name = "refreshToken", required = false) String refreshTokenCookie) {

        System.out.println("▶▶▶ /api/auth/refresh called with header=" + authorizationHeader);

        // ① 로그 찍기
        log.info("Called /api/auth/refresh; Authorization header='{}', refreshTokenCookie='{}'",
                 authorizationHeader, refreshTokenCookie);


        String refreshToken = null;
        if (authorizationHeader != null && authorizationHeader.startsWith("Bearer ")) {
            refreshToken = authorizationHeader.substring("Bearer ".length());
        } else if (refreshTokenCookie != null) {
            refreshToken = refreshTokenCookie;
        } else {
            log.warn("No refresh token provided"); // 추가
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Refresh token is missing");
        }

        String newAccessToken = authService.refreshAccessToken(refreshToken);
        log.debug("New access token generated"); // 추가
        return ResponseEntity.ok(
            AccessTokenResponseDto.builder()
                .accessToken(newAccessToken)
                .build()
        );
        
    }


        /*
         * < -- refresh token의 역할 -- >
         * 로그아웃, 강제 만료 처리
         * 사용자가 로그아웃하거나, 보안상 의심스러운 상황(비밀번호 변경 or 탈퇴 등)이 발생했을 때,
         * 서버 측에서 해당 Refresh Token을 블랙리스트에 등록하거나 DB에서 삭제해 두면
         * 이후 이 토큰으로는 더 이상 Access Token 재발급을 받을 수 없게 되어 세션이 완전히 종료된다.
         * 
         * 또 하나의 Usage는
         * Access Token(JWT)은 보통 짧은 유효기간(ex - 5분~1시간)만을 가지도록 발급한다.
         * 따라서 클라이언트가 이 토큰으로 API 호출 도중 만료 에러(401)을 받으면,
         * Refresh Token을 서버에 보내 새 Access Token을 요청한다.
         * 서버는 유효한 Refresh Token임을 확인한 뒤, 새로운 Access Token을 발급해 준다.
         * 
         * < -- logout 성공 메시지 추가하려면 -- >
         * @PostMapping("/logout")
            public ResponseEntity<LogoutResponseDto> logout(...) {
                String refreshToken = TokenUtils.extractRefreshFrom(bearerToken);
                authService.logout(refreshToken);
                return ResponseEntity.ok(
                    LogoutResponseDto.builder()
                        .message("로그아웃 되었습니다.")
                        .build()
                );
            }
         */

}