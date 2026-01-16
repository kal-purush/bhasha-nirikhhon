package com.unionclass.memberservice.auth.dto.out;

import com.unionclass.memberservice.member.entity.Member;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class MemberUuidResDto {

    private String memberUuid;

    @Builder
    public MemberUuidResDto(String memberUuid) {
        this.memberUuid = memberUuid;
    }

    public static MemberUuidResDto from(Member member) {
        return MemberUuidResDto.builder()
                .memberUuid(member.getMemberUuid())
                .build();
    }
}