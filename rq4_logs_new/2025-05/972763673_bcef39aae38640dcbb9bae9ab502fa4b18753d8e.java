package com.github.classpick.lecture.controller;

import com.github.classpick.lecture.controller.dto.response.LectureListResponse;
import com.github.classpick.lecture.service.LectureService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "강의실 수업")
@RestController
@RequiredArgsConstructor
public class LectureController {

    private final LectureService lectureService;

    @Operation(summary = "강의실 수업 정보 가져오기")
    @GetMapping("/v0.0/lectures/{id}")
    public LectureListResponse lecture(@PathVariable long id) {

        return lectureService.getLectures(id);
    }
}