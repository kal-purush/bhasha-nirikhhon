package com.unionclass.memberservice.auth.dto.in;

import com.unionclass.memberservice.auth.vo.in.NicknameReqVo;
import jakarta.validation.Valid;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class NicknameReqDto {

    private String nickname;

    @Builder
    public NicknameReqDto(String nickname) { this.nickname = nickname; }

    public static NicknameReqDto from(NicknameReqVo nicknameReqVo) {
        return NicknameReqDto.builder()
                .nickname(nicknameReqVo.getNickname())
                .build();
    }
}