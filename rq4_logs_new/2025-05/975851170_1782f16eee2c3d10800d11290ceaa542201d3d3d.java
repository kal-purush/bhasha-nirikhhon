package com.example.demo.service.impl;
import com.example.demo.service.AuthService;

import com.example.demo.domain.RefreshToken;
import com.example.demo.domain.User;
import com.example.demo.dto.AuthRequestDto;
import com.example.demo.dto.AuthResponseDto;
import com.example.demo.exception.CustomException;
import com.example.demo.exception.ErrorCode;
import com.example.demo.repository.RefreshTokenRepository;
import com.example.demo.repository.UserRepository;
import com.example.demo.util.JwtUtils;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import java.time.Instant;


@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {
       
    private final UserRepository            userRepository;
    private final RefreshTokenRepository    refreshTokenRepository;
    private final PasswordEncoder           passwordEncoder;
    private final JwtUtils                  jwtUtils;


    /**
    * 사용자 로그인 처리
    */
    @Override
    public AuthResponseDto login(AuthRequestDto request) {

        String username = request.getUsername();

        // 사용자 조회
        User user = userRepository.findByUsername(username)
            .orElseThrow(() -> new CustomException(ErrorCode.USER_NOT_FOUND));

        // 비밀번호 검증
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new CustomException(ErrorCode.INVALID_CREDENTIALS);
        }

        // Access, Refresh 토큰 생성
        String accessToken  = jwtUtils.generateAccessToken(username);
        String refreshToken = jwtUtils.generateRefreshToken(username);

        // Refresh Token 저장
        RefreshToken rt = RefreshToken.builder()
            .token(refreshToken)
            .username(username)                  // 유효기간 : 7일
            .expiryDate(Instant.now().plusMillis(jwtUtils.getRefreshTokenMillis()))
            .build();
            refreshTokenRepository.save(rt);

        return AuthResponseDto.builder()
            .accessToken(accessToken)
            .refreshToken(refreshToken)
            .build();
    }

    /**
     * 사용자 로그아웃 처리
     * 
     * 전달받은 리프레시 토큰이 DB에 존재하면 삭제
     */
    @Override
    public void logout(String refreshToken) {

        refreshTokenRepository.findByToken(refreshToken).ifPresent(refreshTokenRepository::delete);
    }

    /**
     * 액세스 토큰 재발급 처리
     * 
     * Authorization 헤더 또는 쿠키에서 리프레시 토큰을 추출하고,
     * 유효성을 검사한 후 새 액세스 토큰을 발급
     */
    @Override
    public String refreshAccessToken(String authorizationHeader, String refreshTokenCookie) {

        // JwtUtils를 통해 validation 검사 및 토큰 추출
        String refreshToken = JwtUtils.extractTokenFromHeaderOrCookie(authorizationHeader, refreshTokenCookie);

        // DB에서 토큰 조회
        RefreshToken rt = refreshTokenRepository.findByToken(refreshToken)
            .orElseThrow(() -> new CustomException(ErrorCode.INVALID_TOKEN));
    
        // 만료된 Refresh Token이면 DB에서 삭제, 재로그인 필요
        validateTokenExpiry(rt);

        return jwtUtils.generateAccessToken(rt.getUsername());
    }


// =====================================================
// Helper Methods
// =====================================================

    /**
     * RefreshToken 만료 여부 검사
     * 
     * 토큰이 만료된 경우, DB에서 해당 토큰을 삭제하고 401 예외 처리
     */
    private void validateTokenExpiry(RefreshToken rt) {

        if (rt.getExpiryDate().isBefore(Instant.now())) {

            refreshTokenRepository.delete(rt);

            throw new CustomException(ErrorCode.EXPIRED_TOKEN);
        }
    }

}