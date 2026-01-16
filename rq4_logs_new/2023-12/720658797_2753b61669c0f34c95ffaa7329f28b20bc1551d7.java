package com.jeontongju.product.dto.response;

import com.jeontongju.product.domain.Shorts;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class GetShortsDetailsDto {
    private Long shortsId;
    private String shortsTitle;
    private String shortsDescription;
    private String shortsVideoUrl;
    private String shortsThumbnailImageUrl;
    private Long shortsHits;
    private String targetId;

    public static GetShortsDetailsDto toDto(Shorts shorts) {
        String targetId;
        if (shorts.getProductId() != null) {
            targetId = "product/" + shorts.getProductId();
        } else {
            targetId = "seller/" + shorts.getSellerId();
        }

        return GetShortsDetailsDto.builder()
                .shortsId(shorts.getShortsId())
                .shortsTitle(shorts.getTitle())
                .shortsDescription(shorts.getDescription())
                .shortsVideoUrl(shorts.getVideo())
                .shortsThumbnailImageUrl(shorts.getThumbnail())
                .shortsHits(shorts.getShortsHits())
                .targetId(targetId)
                .build();
    }
}