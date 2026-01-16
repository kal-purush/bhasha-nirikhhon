package com.doubleo.memberservice.domain.auth.service;

import com.doubleo.memberservice.domain.auth.dto.request.LoginRequest;
import com.doubleo.memberservice.domain.auth.dto.response.LoginResponse;
import com.doubleo.memberservice.domain.auth.repository.RefreshTokenRepository;
import com.doubleo.memberservice.domain.member.domain.Member;
import com.doubleo.memberservice.domain.member.repository.MemberRepository;
import com.doubleo.memberservice.global.exception.CommonException;
import com.doubleo.memberservice.global.exception.errorcode.MemberErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService {

    private final MemberRepository memberRepository;
    private final RefreshTokenRepository refreshTokenRepository;
    private final JwtTokenService jwtTokenService;
    private final BCryptPasswordEncoder encoder;

    public LoginResponse loginMember(LoginRequest request) {
        Member member = validateMemberByEmail(request.email());
        if (!encoder.matches(request.password(), member.getPassword())) {
            throw new CommonException(MemberErrorCode.MEMBER_NOT_FOUND);
        }
        return getLoginResponse(member);
    }

    public void logoutMember(String accessTokenValue, Long memberId) {
        validateMemberById(memberId);
        refreshTokenRepository.deleteById(memberId);
        jwtTokenService.putAccessTokenOnBlackList(accessTokenValue);
    }

    private Member validateMemberByEmail(String email) {
        return memberRepository
                .findMemberByEmail(email)
                .orElseThrow(() -> new CommonException(MemberErrorCode.MEMBER_NOT_FOUND));
    }

    private void validateMemberById(Long memberId) {
        memberRepository
                .findById(memberId)
                .orElseThrow(() -> new CommonException(MemberErrorCode.MEMBER_NOT_FOUND));
    }

    private LoginResponse getLoginResponse(Member member) {
        String accessToken = jwtTokenService.createAccessToken(member.getId());
        String refreshToken = jwtTokenService.createRefreshToken(member.getId());
        return LoginResponse.of(accessToken, refreshToken);
    }
}