package com.spartanullnull.otil.domain.like.controller;

import com.spartanullnull.otil.domain.like.service.LikeService;
import com.spartanullnull.otil.domain.user.entity.User;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/likes")
@RequiredArgsConstructor
public class LikeController {

    private final LikeService likeService;

    // 좋아요 누르기 API
    @PostMapping("/posts/{postId}")
    public ResponseEntity<String> createLikeForPost(
        @PathVariable Long postId,
        @RequestBody User user) {
        likeService.createLikeForPost(user, postId);
        return ResponseEntity.ok("좋아요 성공!");
    }

    // 좋아요 취소하기 API
    @DeleteMapping("/post/{postId}")
    public ResponseEntity<String> deleteLikeForPost(
        @PathVariable Long postId,
        @RequestBody User user) {
        likeService.deleteLikeForPost(user, postId);
        return ResponseEntity.ok("좋아요 취소!");
    }

    // 좋아요 개수 조회 API
    @GetMapping("/posts/{postId}/count")
    public ResponseEntity<Long> getLikeCountForPost(@PathVariable Long postId) {
        Long likeCount = likeService.getLikeCountForPost(postId);
        return ResponseEntity.ok(likeCount);
    }
}