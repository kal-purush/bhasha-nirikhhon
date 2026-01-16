package com.boot.Config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import com.boot.DTO.UsertbDTO;
import com.boot.Service.LoginService;

@Component
public class AuthProvider implements AuthenticationProvider {

    @Autowired
    private LoginService loginservice;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String userid = (String) authentication.getName(); 
        String password = (String) authentication.getCredentials(); 

        UsertbDTO userdto = loginservice.getUserById(userid);
        if (userdto == null || userdto.getPpass() == null) {
            throw new BadCredentialsException("Invalid username or password");
        }
        if (passwordEncoder.matches(password, userdto.getPpass())) {
            // 로그인 성공 로직
        } else {
            throw new BadCredentialsException("Invalid username or password");
        }


        List<GrantedAuthority> roles = new ArrayList<>();
        roles.add(new SimpleGrantedAuthority("USER"));

        return new UsernamePasswordAuthenticationToken(userdto.getUserid(), null, roles);
    }


    @Override
    public boolean supports(Class<?> authentication) {
        // 지원하는 인증 방식 설정
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}