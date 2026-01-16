package com.Kidari.server.domain.member.dto;


import com.Kidari.server.domain.member.entity.Member;
import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class MemberHomeResDto {
    private String nickname; // 엔티티의 login
    private Long snowflake;
    private Long snowmanHeight;
    private Long snowId;
    private Long hatId;
    private Long decoId;
    private boolean newAlarm;

    public MemberHomeResDto(Member member) {
        this.nickname = member.getLogin();
        this.snowflake = member.getSnowflake();
        this.snowmanHeight = member.getSnowmanHeight();
        this.snowId = member.getItem().getSnowId();
        this.hatId = member.getItem().getHatId();
        this.decoId = member.getItem().getDecoId();
        this.newAlarm = member.getNewAlarm();
    }
}