package com.gxdxx.instagram.service;

import com.gxdxx.instagram.entity.RefreshToken;
import com.gxdxx.instagram.exception.RefreshTokenNotFoundException;
import com.gxdxx.instagram.repository.RefreshTokenRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Service
public class RefreshTokenService {

    private final RefreshTokenRepository refreshTokenRepository;

    public RefreshToken findByRefreshToken(String refreshToken) {
        return refreshTokenRepository.findByRefreshToken(refreshToken)
                .orElseThrow(RefreshTokenNotFoundException::new);
    }

}