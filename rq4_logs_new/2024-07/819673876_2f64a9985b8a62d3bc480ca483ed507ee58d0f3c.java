package com.example.demo.Landmark.controller;

import com.example.demo.Landmark.dto.LandmarkLikeDTO;
import com.example.demo.Landmark.entity.Landmark;
import com.example.demo.Landmark.entity.LandmarkLike;
import com.example.demo.Landmark.repository.LandmarkLikeRepository;
import com.example.demo.Landmark.service.LandmarkLikeService;
import com.example.demo.member.entity.Member;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.List;

@RestController
@RequestMapping("/landmarkLike")
public class LandmarkLikeController {

    @Autowired
    LandmarkLikeService landmarkLikeService;

    @Autowired
    LandmarkLikeRepository landmarkLikeRepository;

    @PostMapping("/isLike")
    public ResponseEntity<String> toggleLike(@RequestBody LandmarkLikeDTO dto,
                                             Principal principal){
        String id =  principal.getName();   //인증된 ID가져오고

        Landmark landmark = new Landmark();
        landmark.setLandmarkNo(dto.getLandmarkNo());

        Member user = new Member();
        user.setUserId(dto.getUserId());

        landmarkLikeService.toggleLike(landmark,user);
        return ResponseEntity.ok("islike");
    }

    @GetMapping("/list")
    public ResponseEntity<List<LandmarkLike>> getAllLikesByUser(@RequestParam String userId) {
        Member user = new Member();
        user.setUserId(userId);

        List<LandmarkLike> likes = landmarkLikeService.getAllLikesByUser(user);
        return ResponseEntity.ok(likes);
    }
    // 특정 랜드마크의 좋아요 수 조회 *** @PathVariable는 landmarkNo를 파라미터로 받아와서 사용할수 있어
    @GetMapping("/{landmarkNo}/likes")
    public ResponseEntity<Integer> getLikesCount(@PathVariable int landmarkNo) {
        int likeCount = landmarkLikeRepository.countIsLikeTrue(landmarkNo);
        return ResponseEntity.ok(likeCount);
    }
}