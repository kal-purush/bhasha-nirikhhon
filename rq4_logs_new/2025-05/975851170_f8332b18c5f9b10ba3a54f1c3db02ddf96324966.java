package com.example.demo.service;

import org.springframework.stereotype.Service;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;


import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import com.example.demo.domain.User;
import com.example.demo.domain.RefreshToken;
import com.example.demo.repository.UserRepository;
import com.example.demo.repository.RefreshTokenRepository;
import com.example.demo.util.JwtUtils;
import com.example.demo.dto.AuthRequestDto;
import com.example.demo.dto.AuthResponseDto;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class AuthService {
    private final UserRepository            userRepository;
    private final PasswordEncoder           passwordEncoder;
    private final JwtUtils                  jwtUtils;
    private final RefreshTokenRepository    rfTokenRepo;

    /**
     * 리프레시 토큰으로 새로운 액세스 토큰만 발급합니다.
     * @param refreshToken 클라이언트가 보낸 리프레시 토큰 문자열
     * @return 새로 발급된 액세스 토큰
     */
    public String refreshAccessToken(String refreshToken) {
        // DB에서 토큰 조회
        RefreshToken rt = rfTokenRepo.findByToken(refreshToken)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid refresh token"));
    
        // 만료 여부 검사
        if (rt.getExpiryDate().isBefore(Instant.now())) {
            // 만료된 토큰은 삭제하고 재로그인 유도
            rfTokenRepo.delete(rt);
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Refresh token expired");
        }

        // 새 액세스 토큰 생성 및 반환
        return jwtUtils.generateAccessToken(rt.getUsername());
    }



    public AuthResponseDto login(AuthRequestDto request) {
        // 1) 사용자 조회
        User user = userRepository.findByUsername(request.getUsername())
            .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다."));

        // 2) 비밀번호 검증
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new RuntimeException("비밀번호가 일치하지 않습니다.");
        }

        // 3) 토큰 생성
        String accessToken  = jwtUtils.generateAccessToken(request.getUsername());
        String refreshToken = jwtUtils.generateRefreshToken(request.getUsername());

        // 4) 리프레시 토큰 저장
        RefreshToken rt = RefreshToken.builder()
            .token(refreshToken)
            .username(request.getUsername())
            .expiryDate(Instant.now().plusMillis(jwtUtils.getRefreshTokenMillis()))
            .build();
            rfTokenRepo.save(rt);

        // 5) 응답 반환
        return AuthResponseDto.builder()
            .accessToken(accessToken)
            .refreshToken(refreshToken)
            .build();
    }

    // 리프레시 토큰 DB에서 삭제
    public void logout(String refreshToken) {
        rfTokenRepo.findByToken(refreshToken)
            .ifPresent(rfTokenRepo::delete);
    }

}