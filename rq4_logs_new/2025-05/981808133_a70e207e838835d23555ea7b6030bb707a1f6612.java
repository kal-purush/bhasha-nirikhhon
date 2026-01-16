package com.unionclass.memberservice.oauth.application;

import com.unionclass.memberservice.auth.dto.out.SignInResDto;
import com.unionclass.memberservice.auth.util.AuthUtils;
import com.unionclass.memberservice.common.exception.BaseException;
import com.unionclass.memberservice.common.exception.ErrorCode;
import com.unionclass.memberservice.common.security.CustomUserDetailsService;
import com.unionclass.memberservice.oauth.dto.in.BindOAuthAccountReqDto;
import com.unionclass.memberservice.oauth.dto.in.ProviderReqDto;
import com.unionclass.memberservice.oauth.entity.MemberOAuth;
import com.unionclass.memberservice.oauth.infrastructure.MemberOAuthRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class MemberOAuthServiceImpl implements  MemberOAuthService {

    private final MemberOAuthRepository memberOAuthRepository;
    private final CustomUserDetailsService customUserDetailsService;
    private final AuthUtils authUtils;

    /**
     * /api/v1/oauth
     *
     * 1. 소셜 로그인
     * 2. OAuth 계정 연동
     */

    /**
     * 1. 소셜 로그인
     *
     * @param providerReqDto
     * @return
     */
    @Transactional
    @Override
    public SignInResDto signInWithOAuth(ProviderReqDto providerReqDto) {

        MemberOAuth memberOAuth = memberOAuthRepository.findByProviderAndProviderAccountId(
                providerReqDto.getProvider(),
                providerReqDto.getProviderAccountId()
        ).orElseThrow(() -> new BaseException(ErrorCode.NO_EXIST_OAUTH_MEMBER));
        
        UserDetails userDetails = customUserDetailsService.loadUserByUsername(memberOAuth.getMemberUuid());
        UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
                userDetails, null, userDetails.getAuthorities()
        );
        SecurityContextHolder.getContext().setAuthentication(authentication);
        String accessToken = authUtils.createToken(authentication);

        return SignInResDto.of(memberOAuth.getMemberUuid(), accessToken);
    }

    /**
     * 2. OAuth 계정 연동
     *
     * @param bindOAuthAccountReqDto
     */
    @Transactional
    @Override
    public void bindOAuthAccount(BindOAuthAccountReqDto bindOAuthAccountReqDto) {

        if (memberOAuthRepository.existsByProviderAndProviderAccountId(
                bindOAuthAccountReqDto.getProvider(),
                bindOAuthAccountReqDto.getProviderAccountId()
        )) {
            log.warn("이미 연동된 OAuth 계정이 존재 - provider: {}, accountId: {}",
                    bindOAuthAccountReqDto.getProvider(), bindOAuthAccountReqDto.getProviderAccountId());
            throw new BaseException(ErrorCode.OAUTH_ACCOUNT_ALREADY_BOUND);
        }

        memberOAuthRepository.save(bindOAuthAccountReqDto.toEntity());
        log.info("OAuth 계정 연동 완료 - provider: {}, accountId: {}, memberUuid: {}",
                bindOAuthAccountReqDto.getProvider(),
                bindOAuthAccountReqDto.getProviderAccountId(),
                bindOAuthAccountReqDto.getMemberUuid()
        );
    }
}