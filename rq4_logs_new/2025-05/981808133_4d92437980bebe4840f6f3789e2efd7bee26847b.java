package com.unionclass.memberservice.auth.dto.in;

import com.unionclass.memberservice.auth.vo.in.LoginIdReqVo;
import jakarta.validation.Valid;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class LoginIdReqDto {

    private String loginId;

    @Builder
    public LoginIdReqDto(String loginId) { this.loginId = loginId; }

    public static LoginIdReqDto from(LoginIdReqVo loginIdReqVo) {
        return LoginIdReqDto.builder()
                .loginId(loginIdReqVo.getLoginId())
                .build();
    }
}