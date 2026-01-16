package com.roomfit.be.participation.application.dto;

import com.roomfit.be.user.domain.User;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ParticipantDTO {
    private Long id;
    private String nickname;
    private String college;
    public static ParticipantDTO of(User participant){
        return ParticipantDTO.builder()
                .id(participant.getId())
                .nickname(participant.getNickname())
                .college(participant.getCollege())
                .build();
    }
}