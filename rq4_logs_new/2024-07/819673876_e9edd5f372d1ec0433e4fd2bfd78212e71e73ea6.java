package com.example.demo.service;

import com.example.demo.Landmark.entity.Landmark;
import com.example.demo.course.entity.CourseLandmark;
import com.example.demo.course.service.CourseLikeService;
import com.example.demo.member.entity.Member;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class CourseLikeServiceTest {

    @Autowired
    CourseLikeService service;

    @Test
    public void 전체코스좋아요true_취소false(){
        CourseLandmark courseLandmark = CourseLandmark.builder().courseNo().build();
        Member member = Member.builder().userId("green").build();
        service.toggleLikeCourse(courseLandmark,member);
    }
//    courseLandmark완성되어야 테스트가능
}