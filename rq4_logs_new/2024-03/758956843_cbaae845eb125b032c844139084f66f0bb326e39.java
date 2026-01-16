package com.example.campuschool_backend.dto.lecture;

import com.example.campuschool_backend.domain.lecture.Review;
import com.example.campuschool_backend.domain.lecture.enums.CategoryType;
import com.example.campuschool_backend.domain.lecture.enums.Difficulty;
import com.example.campuschool_backend.domain.lecture.enums.LectureStatus;
import lombok.*;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class ReviewCardDTO {
    private Long id;
    private String title;
    private CategoryType categoryType;
    private String refImage;
    private String teacherName;
    private int rating;
    private String reviewer;
    private String content;
    private LocalDateTime createdAt;
    public static ReviewCardDTO from (Review review) {
        return ReviewCardDTO.builder()
                .id(review.getLecture().getId())
                .title(review.getLecture().getTitle())
                .categoryType(review.getLecture().getCategoryType())
                .refImage(review.getLecture().getRefImage())
                .teacherName(review.getLecture().getTeacher().getName())
                .reviewer(review.getUser().getName())
                .rating(review.getRating())
                .content(review.getContent())
                .createdAt(review.getCreatedAt())
                .build();
    }

}