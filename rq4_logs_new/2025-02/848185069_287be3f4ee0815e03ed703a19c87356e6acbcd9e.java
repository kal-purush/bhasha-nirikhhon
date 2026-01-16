package com.rljj.chipservice.domain.chippost.dto;

import com.rljj.switchswitchentity.chip.chippost.ChipPost;
import lombok.*;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class PostResponse {
    private Long id;
    private Long chipInfoId;
    private Long memberId;
    private String title;
    private String description;
    private String status;

    public static PostResponse from(ChipPost chipPost) {
        return new PostResponse(
                chipPost.getId(),
                chipPost.getChipInfo().getId(),
                chipPost.getMember().getId(),
                chipPost.getTitle(),
                chipPost.getDescription(),
                chipPost.getStatus().name()
        );
    }
}