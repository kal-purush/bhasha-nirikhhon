package com.doubleo.memberservice.domain.auth.service;

import com.doubleo.memberservice.domain.auth.domain.PrincipalDetails;
import com.doubleo.memberservice.domain.member.domain.Member;
import com.doubleo.memberservice.domain.member.repository.MemberRepository;
import com.doubleo.memberservice.global.exception.CommonException;
import com.doubleo.memberservice.global.exception.errorcode.MemberErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PrincipalDetailsService implements UserDetailsService {

    private final MemberRepository memberRepository;

    @Override
    public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
        Member member = memberRepository.findMemberByEmail(email);
        if (member != null) {
            return new PrincipalDetails(member);
        }
        throw new CommonException(MemberErrorCode.MEMBER_NOT_FOUND);
    }
}