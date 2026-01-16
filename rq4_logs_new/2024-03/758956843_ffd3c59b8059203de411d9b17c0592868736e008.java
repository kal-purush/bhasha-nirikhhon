package com.example.campuschool_backend.dto.lecture;

import com.example.campuschool_backend.domain.lecture.AvaliableTime;
import com.example.campuschool_backend.domain.lecture.CurriculumEntity;
import com.example.campuschool_backend.domain.lecture.Review;
import com.example.campuschool_backend.domain.lecture.enums.CategoryType;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class LectureDetailDTO {
    private String teacherName;
    private String teacherImage;
    private String teacherDescription;
    private String teacherEducation;
    private String teacherHistory;
    private String lectureTitle;
    private CategoryType categoryType;
    private String lectureImage;
    private String lectureDescription;
    private int day;
    private List<Review> reviewList;
    private List<CurriculumEntity> curriculumEntityList;
    private List<AvaliableTime> avaliableTimeList;
}