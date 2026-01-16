package com.example.demo.Landmark.controller;

import com.example.demo.Landmark.entity.Landmark;
import com.example.demo.Landmark.entity.LandmarkLike;
import com.example.demo.Landmark.service.LandmarkLikeService;
import com.example.demo.member.entity.Member;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/landmarkLike")
public class LandmarkLikeController {

    @Autowired
    LandmarkLikeService landmarkLikeService;

    @PostMapping("/isLike")
    public ResponseEntity<String> toggleLike(@RequestParam(name = "landmarkNo") int landmarkNo, @RequestParam(name = "userId") String userId){
        Landmark landmark = new Landmark();
        landmark.setLandmarkNo(landmarkNo);

        Member user = new Member();
        user.setUserId(userId);

        landmarkLikeService.toggleLike(landmark, user);
        return ResponseEntity.ok("islike");
    }
    @GetMapping("/list")
    public ResponseEntity<List<LandmarkLike>> getAllLikesByUser(@RequestParam String userId) {
        Member user = new Member();
        user.setUserId(userId);

        List<LandmarkLike> likes = landmarkLikeService.getAllLikesByUser(user);
        return ResponseEntity.ok(likes);
    }
}