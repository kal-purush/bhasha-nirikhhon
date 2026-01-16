package com.example.demo.course.controller;

import com.example.demo.course.dto.CourseLikeDTO;
import com.example.demo.course.entity.CourseLandmark;
import com.example.demo.course.entity.CourseLike;
import com.example.demo.course.repository.CourseLikeRepository;
import com.example.demo.course.service.CourseLikeService;
import com.example.demo.member.entity.Member;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.List;

@RestController
@RequestMapping("/courseLike")
public class CourseLikeController {
    @Autowired
    CourseLikeService courseLikeService;


    @PostMapping("isLike")
    public ResponseEntity<String> toggleLike(@RequestBody CourseLikeDTO dto,
                                             Principal principal){
        CourseLandmark courseLandmark = new CourseLandmark();
        courseLandmark.setCourseNo(dto.getCourseNo());

        Member user = new Member();
        user.setUserId(principal.getName());

        courseLikeService.toggleLikeCourse(courseLandmark,user);

        return ResponseEntity.ok("isLike");
    }

    @GetMapping("list")
    public ResponseEntity<List<CourseLike>> getAllLikeAndByUser(@RequestParam String userId){
        Member user = new Member();
        user.setUserId(userId);

        List<CourseLike> likes = courseLikeService.getAllLikesByUser(user);
        return ResponseEntity.ok(likes);
    }

    @GetMapping("/{courseNo}/likes")
    public ResponseEntity<Integer> getLikesCount(@PathVariable int courseNo){
        int likeCount = courseLikeService.likeCount(courseNo);
        return ResponseEntity.ok(likeCount);
    }
}