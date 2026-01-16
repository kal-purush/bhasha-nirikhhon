package com.cakestation.backend.review.controller.dto.response;

import com.cakestation.backend.review.domain.Review;
import com.cakestation.backend.review.service.dto.ReviewDto;
import lombok.*;

import java.time.LocalDate;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ReviewResponse {
    private String username;
    private int cakeNumber;
    private String sheetType;
    private String requestOption;
    private List<String> reviewImages;
    private String content;
    private LocalDate createdDate;

    public static ReviewResponse from(ReviewDto reviewDto){
        return ReviewResponse.builder()
                .username(reviewDto.getUsername())
                .cakeNumber(reviewDto.getCakeNumber())
                .sheetType(reviewDto.getSheetType())
                .requestOption(reviewDto.getRequestOption())
                .reviewImages(reviewDto.getReviewImages())
                .content(reviewDto.getContent())
                .createdDate(reviewDto.getCreatedDate())
                .build();
    }
}