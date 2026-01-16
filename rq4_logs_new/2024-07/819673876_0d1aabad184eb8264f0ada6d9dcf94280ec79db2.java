package com.example.demo.Landmark_Course.controller;

import com.example.demo.Landmark_Course.dto.LCMappingDTO;
import com.example.demo.Landmark_Course.service.LCMappingService;
import com.example.demo.course.dto.CourseLandmarkDTO;
import com.example.demo.course.service.CourseLandmarkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/lc")
public class LCController {

    @Autowired
    LCMappingService service;

    @Autowired
    CourseLandmarkService courseLandmarkService;

    @PostMapping("/register")
    public ResponseEntity<Integer> register(@RequestBody LCMappingDTO dto,
                                            @RequestParam(name = "id")String id
                                            ){

        CourseLandmarkDTO courseLandmark = CourseLandmarkDTO.builder()
                .courseNo(dto.getCourseNo())
                .userId(id)
                .build();

        courseLandmarkService.register(courseLandmark);

        int no = service.register(dto);

        return new ResponseEntity<>(no, HttpStatus.OK);
    }
}