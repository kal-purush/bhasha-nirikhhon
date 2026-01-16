package com.example.demo.course.controller;

import com.example.demo.course.dto.CourseLandmarkDTO;
import com.example.demo.course.service.CourseLandmarkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/course")
public class CourseLandmarkController {

    @Autowired
    CourseLandmarkService service;

    @GetMapping("/read")
    public ResponseEntity<List> readById(@RequestParam(name = "id")String userId){

        List<CourseLandmarkDTO> list = service.readByUserId(userId);
        return new ResponseEntity<>(list, HttpStatus.OK);
    }
}