package com.unionclass.memberservice.auth.util;

import com.unionclass.memberservice.common.security.JwtProvider;
import com.unionclass.memberservice.member.entity.Member;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AuthUtils {

    private final JwtProvider jwtProvider;
    private final AuthenticationManager authenticationManager;

    /**
     * 토큰 생성
     * @param authentication
     * @return
     */
    public String createToken(Authentication authentication) {
        return jwtProvider.generateAccessToken(authentication);
    }

    /**
     * 인증
     * @param member
     * @param inputPassword
     * @return
     */
    public Authentication authenticate(Member member, String inputPassword) {
        return authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(member.getMemberUuid(), inputPassword));
    }
}