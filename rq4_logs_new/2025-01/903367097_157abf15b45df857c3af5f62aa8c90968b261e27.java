package com.api.knowknowgram.common.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import com.api.knowknowgram.entity.Users;
import com.api.knowknowgram.repository.UserRepository;


@Component
public class CustomAuthenticationProvider implements AuthenticationProvider {

    @Autowired
    private UserRepository userRepository; // 사용자 정보 저장소

    @Autowired
    private PasswordEncoder passwordEncoder; // PasswordEncoder 주입

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String email = authentication.getName(); // 사용자의 이메일
        Object credentials = authentication.getCredentials(); // 비밀번호 또는 Provider ID
    
        if (credentials == null) {
            throw new BadCredentialsException("Credentials cannot be null");
        }
    
        String credentialString = credentials.toString();
        
        // 1. 이메일 + 비밀번호 인증 (개발용)
        // if (credentialString.equals("1234")) {
        //     Users user = userRepository.findByEmail(email)
        //             .orElseThrow(() -> new UsernameNotFoundException("User not found with email"));
    
        //     // 비밀번호 검증
        //     if (!passwordEncoder.matches(credentialString, user.getPassword())) {
        //         throw new BadCredentialsException("Invalid password");
        //     }
    
        //     UserDetailsImpl userDetailsImpl = UserDetailsImpl.build(user);

        //     // 인증 객체 반환 (비밀번호 기반 인증)
        //     return new UsernamePasswordAuthenticationToken(userDetailsImpl, credentialString, userDetailsImpl.getAuthorities());
        // } else {    // 2. 이메일 + Provider ID 인증
        //     Users user = userRepository.findByEmailAndProviderId(email, credentialString)
        //             .orElseThrow(() -> new UsernameNotFoundException("User not found with email and provider ID"));

        //     UserDetailsImpl userDetailsImpl = UserDetailsImpl.build(user);
    
        //     // 인증 객체 반환 (Provider ID 기반 인증)
        //     return new UsernamePasswordAuthenticationToken(userDetailsImpl, credentialString, userDetailsImpl.getAuthorities());
        // }

        Users user = userRepository.findByEmailAndProviderId(email, credentialString)
                    .orElseThrow(() -> new UsernameNotFoundException("User not found with email and provider ID"));

        UserDetailsImpl userDetailsImpl = UserDetailsImpl.build(user);

        // 인증 객체 반환 (Provider ID 기반 인증)
        return new UsernamePasswordAuthenticationToken(userDetailsImpl, credentialString, userDetailsImpl.getAuthorities());
    }
    

    @Override
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}