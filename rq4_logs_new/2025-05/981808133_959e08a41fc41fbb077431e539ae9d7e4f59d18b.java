package com.unionclass.memberservice.auth.application;

import com.unionclass.memberservice.auth.dto.in.SignUpReqDto;
import com.unionclass.memberservice.common.exception.BaseException;
import com.unionclass.memberservice.common.exception.ErrorCode;
import com.unionclass.memberservice.common.jwt.JwtProvider;
import com.unionclass.memberservice.common.util.NumericUuidGenerator;
import com.unionclass.memberservice.member.entity.Member;
import com.unionclass.memberservice.member.infrastructure.MemberRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AuthServiceImpl implements AuthService {

    private final MemberRepository memberRepository;
    private final JwtProvider jwtProvider;
    private final AuthenticationManager authenticationManager;
    private final PasswordEncoder passwordEncoder;
    private final NumericUuidGenerator numericUuidGenerator;

    /**
     * 1. 회원가입
     * 2. 로그인
     */

    /**
     * 1. 회원가입
     * @param signUpReqDto
     */
    @Transactional
    @Override
    public void signUp(SignUpReqDto signUpReqDto) {
        try {
            memberRepository.save(
                    signUpReqDto.toEntity(
                            numericUuidGenerator.generate(),
                            passwordEncoder.encode(signUpReqDto.getPassword())
                    )
            );
        } catch (Exception e) {
            throw new BaseException(ErrorCode.FAILED_TO_SIGN_UP);
        }
    }

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
                new UsernamePasswordAuthenticationToken(
                        member.getMemberUuid(),
                        inputPassword
                )
        );
    }
}