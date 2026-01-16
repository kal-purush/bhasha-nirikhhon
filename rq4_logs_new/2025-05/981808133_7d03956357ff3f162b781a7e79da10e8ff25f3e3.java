package com.unionclass.memberservice.auth.dto.in;

import com.unionclass.memberservice.auth.vo.in.GetLoginIdReqVo;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class GetLoginIdReqDto {

    private String loginId;

    @Builder
    public GetLoginIdReqDto(String loginId) { this.loginId = loginId; }

    public static GetLoginIdReqDto from(GetLoginIdReqVo loginIdReqVo) {
        return GetLoginIdReqDto.builder()
                .loginId(loginIdReqVo.getLoginId())
                .build();
    }
}