package com.unionclass.memberservice.auth.dto.in;

import com.unionclass.memberservice.auth.vo.in.SignInReqVo;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class SignInReqDto {

    private String loginId;
    private String password;

    @Builder
    public SignInReqDto(String loginId, String password) {
        this.loginId = loginId;
        this.password = password;
    }

    public static SignInReqDto from(SignInReqVo signInReqVo) {
        return SignInReqDto.builder()
                .loginId(signInReqVo.getLoginId())
                .password(signInReqVo.getPassword())
                .build();
    }
}