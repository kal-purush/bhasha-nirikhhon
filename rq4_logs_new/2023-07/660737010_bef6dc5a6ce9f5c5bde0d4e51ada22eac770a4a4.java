package com.example.oneulnail.domain.user.mapper;

import com.example.oneulnail.domain.user.dto.request.SignUpReqDto;
import com.example.oneulnail.domain.user.dto.response.SignInResDto;
import com.example.oneulnail.domain.user.dto.response.SignUpResDto;
import com.example.oneulnail.domain.user.entity.User;
import com.example.oneulnail.global.config.security.JwtTokenProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SignMapper {

    private final JwtTokenProvider jwtTokenProvider;

    public SignUpResDto signUpEntityToDto(){
        return SignUpResDto.builder()
                .msg("회원가입에 성공했습니다.")
                .build();
    }

    public SignInResDto signInEntityToDto(User user){
        return SignInResDto.builder()
                .msg("로그인에 성공했습니다.")
                .token(jwtTokenProvider.createToken(String.valueOf(user.getPhoneNum()),
                        user.getRole().toString()))
                .build();

    }

}