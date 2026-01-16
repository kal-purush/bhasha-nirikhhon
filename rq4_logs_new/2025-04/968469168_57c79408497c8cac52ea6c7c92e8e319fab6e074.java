package com.doubleo.memberservice.domain.auth.service;


import com.doubleo.memberservice.domain.auth.domain.BlackListToken;
import com.doubleo.memberservice.domain.auth.domain.RefreshToken;
import com.doubleo.memberservice.domain.auth.dto.AccessTokenDto;
import com.doubleo.memberservice.domain.auth.dto.RefreshTokenDto;
import com.doubleo.memberservice.domain.auth.repository.BlackListTokenRepository;
import com.doubleo.memberservice.domain.auth.repository.RefreshTokenRepository;
import com.doubleo.memberservice.global.util.JwtUtil;
import io.jsonwebtoken.ExpiredJwtException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class JwtTokenService {

    private final JwtUtil jwtUtil;
    private final RefreshTokenRepository refreshTokenRepository;
    private final BlackListTokenRepository blackListTokenRepository;

    public AccessTokenDto createAccessTokenDto(Long memberId) {
        return jwtUtil.generateAccessTokenDto(memberId);
    }

    public String createAccessToken(Long memberId) {
        return jwtUtil.generateAccessToken(memberId);
    }

    public String createRefreshToken(Long memberId) {
        String token = jwtUtil.generateRefreshToken(memberId);
        RefreshToken refreshToken =
                RefreshToken.builder()
                        .memberId(memberId)
                        .token(token)
                        .ttl(jwtUtil.getRefreshTokenExpirationTime())
                        .build();
        refreshTokenRepository.save(refreshToken);

        return token;
    }

    public RefreshTokenDto retrieveRefreshToken(String refreshTokenValue) {
        RefreshTokenDto refreshTokenDto = parseRefreshToken(refreshTokenValue);

        if (refreshTokenDto == null) {
            return null;
        }

        Optional<RefreshToken> refreshToken = getRefreshToken(refreshTokenDto.memberId());

        if (refreshToken.isPresent() &&
                refreshTokenDto.refreshTokenValue().equals(refreshToken.get().getToken())) {
            return refreshTokenDto;
        }

        return null;
    }

    public AccessTokenDto reissueAccessTokenIfExpired(String accessTokenValue) {
        try {
            jwtUtil.parseAccessToken(accessTokenValue);
            return null;
        } catch (ExpiredJwtException e) {
            Long memberId = Long.parseLong(e.getClaims().getSubject());

            return createAccessTokenDto(memberId);
        }
    }

    public void putAccessTokenOnBlackList(String accessTokenValue) {

        String accessToken = jwtUtil.resolveToken(accessTokenValue);
        if (accessToken == null) {
            return;
        }

        long remainingMs = jwtUtil.getRemainingExpirationMillis(accessToken);
        long ttlSeconds  = remainingMs > 0 ? remainingMs / 1000 : 0;

        BlackListToken black = BlackListToken.createBlackListToken(accessToken, ttlSeconds);
        blackListTokenRepository.save(black);
    }

    private RefreshTokenDto parseRefreshToken(String refreshTokenValue) {
        try {
            return jwtUtil.parseRefreshToken(refreshTokenValue);
        } catch (Exception e) {
            return null;
        }
    }

    private Optional<RefreshToken> getRefreshToken(Long memberId) {
        return refreshTokenRepository.findById(memberId);
    }
}