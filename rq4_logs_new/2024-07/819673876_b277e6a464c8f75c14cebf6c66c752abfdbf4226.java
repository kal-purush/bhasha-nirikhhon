package com.example.demo.Landmark.controller;

import com.example.demo.Landmark.dto.LandmarkDTO;
import com.example.demo.Landmark.service.LandmarkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/landmark")
public class LandmarkController {

    @Autowired
    LandmarkService landmarkService;

    @ResponseBody
    @PostMapping("/register")
    public ResponseEntity<Integer> registerMyCourse(LandmarkDTO dto){
        int no =landmarkService.register(dto);
// 처음부터 바로 컨트롤러부터 만들려고 하니까 안되나봄, ,,, 포스트맨에서 시도했을때 유저접근권한 없음까지는 확인!
        // TEST 폴더에서 레파짓토리에 저장되는거 확인! 내일 service로도 저장되는지 확인하고 유저 접근권한 주는법 다시 트라이 해보기
        return new ResponseEntity<>(no, HttpStatus.OK);
    }
}