package com.pet.pet_funeral.domain.user.service.impl;

import com.pet.pet_funeral.domain.user.model.LoginType;
import com.pet.pet_funeral.domain.user.model.Role;
import com.pet.pet_funeral.domain.user.model.User;
import com.pet.pet_funeral.domain.user.repository.UserRepository;
import com.pet.pet_funeral.domain.user.service.RefreshTokenService;
import com.pet.pet_funeral.security.dto.AccessTokenPayload;
import com.pet.pet_funeral.security.dto.LoginResponse;
import com.pet.pet_funeral.security.dto.RefreshTokenPayload;
import com.pet.pet_funeral.security.jwt.JwtService;
import com.pet.pet_funeral.security.service.CookieService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseCookie;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Service
@RequiredArgsConstructor
public abstract class SocialLoginServiceImpl implements SocialLoginService{

    private final UserRepository userRepository;
    private final JwtService jwtService;
    private final CookieService cookieService;
    private final RefreshTokenService refreshTokenService;

    /**
     * 외부에서 건드는 login 메서드만 public 으로
     */
    @Override
    @Transactional
    public LoginResponse login(String code) {
        String socialToken = getToken(code);
        String socialId = String.valueOf(getUser(socialToken));

        User user = userRepository.findBySocialId(socialId)
                .orElseGet(() -> register(socialId));

        String accessToken = jwtService.createAccessToken(new AccessTokenPayload(user.getId(), Role.USER,new Date()));
        String refreshToken = jwtService.createRefreshToken(new RefreshTokenPayload(user.getId(),new Date()));
        ResponseCookie responseCookie = cookieService.createRefreshTokenCookie(refreshToken);
        // 로그인하고 이미 DB 에 저장된 유저가 다시 로그인할 때 리프레시값이 중복되지 않으려면 수정 로직 필요
        refreshTokenService.updateRefreshToken(user.getId(), refreshToken);
        return new LoginResponse(Role.USER,accessToken,responseCookie);
    }

    @Override
    public User register(String socialId) {
        User user = User.builder()
                .socialId(socialId)
                .loginType(getLoginType())
                .role(Role.USER)
                .build();
        return userRepository.save(user);
    }

    @Override
    public abstract LoginType getLoginType();

    protected abstract String getToken(String code);
    protected abstract String getUser(String accessToken);

}