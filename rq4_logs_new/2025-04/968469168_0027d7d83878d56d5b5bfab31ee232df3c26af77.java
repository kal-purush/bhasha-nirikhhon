package com.doubleo.memberservice.domain.auth.controller;

import com.doubleo.memberservice.domain.auth.dto.AccessTokenDto;
import com.doubleo.memberservice.domain.auth.dto.RefreshTokenDto;
import com.doubleo.memberservice.domain.auth.dto.request.LoginRequest;
import com.doubleo.memberservice.domain.auth.dto.response.LoginResponse;
import com.doubleo.memberservice.domain.auth.service.AuthService;
import com.doubleo.memberservice.domain.auth.service.JwtTokenService;
import com.doubleo.memberservice.global.util.CookieUtil;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.WebUtils;

@RestController
@RequiredArgsConstructor
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;
    private final CookieUtil cookieUtil;
    private final JwtTokenService jwtTokenService;

    @PostMapping("/login")
    public ResponseEntity<LoginResponse> memberLogin(@RequestBody LoginRequest request){
        LoginResponse response  = authService.loginMember(request);
        String refreshToken = response.refreshToken();
        HttpHeaders headers = cookieUtil.generateRefreshTokenCookie(refreshToken);

        return ResponseEntity.ok().headers(headers).body(response);
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> memberLogout(@RequestHeader(HttpHeaders.AUTHORIZATION) String authorizationHeader,
                                             @RequestHeader("X-User-Id") Long memberId,
                                             HttpServletResponse response){
        authService.logoutMember(authorizationHeader, memberId);

        // 2) RefreshToken 쿠키를 즉시 만료시키는 Set-Cookie 헤더
        ResponseCookie clearCookie = ResponseCookie.from("refreshToken", "")
                .httpOnly(true)
                .secure(true)
                .path("/")
                .maxAge(0)            // 즉시 만료
                .sameSite("Strict")
                .build();
        response.addHeader(HttpHeaders.SET_COOKIE, clearCookie.toString());

        // 3) 바디 없이 204 No Content 반환
        return ResponseEntity.noContent().build();
    }
    }

    @PostMapping("/reissue")
    public ResponseEntity<Void> tokenReissue(HttpServletRequest request, HttpServletResponse response) {
        String oldAccessToken = extractAccessTokenFromHeader(request);
        String refreshToken = extractRefreshTokenFromCookie(request);

        RefreshTokenDto refreshTokenDto = jwtTokenService.retrieveRefreshToken(refreshToken);
        if (refreshTokenDto == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        AccessTokenDto newAccessTokenDto = jwtTokenService.reissueAccessTokenIfExpired(oldAccessToken);
        response.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + newAccessTokenDto.accessTokenValue());

        return ResponseEntity.ok().build();
    }


    private String extractRefreshTokenFromCookie(HttpServletRequest request) {
        Cookie cookie = WebUtils.getCookie(request, "refreshToken");
        return (cookie != null) ? cookie.getValue() : null;
    }
    private String extractAccessTokenFromHeader(HttpServletRequest request) {
        String authorizationHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorizationHeader != null && authorizationHeader.startsWith("Bearer ")) {
            return authorizationHeader.substring(7);
        }
        return null;
    }

}