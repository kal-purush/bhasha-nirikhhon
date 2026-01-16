package com.spartanullnull.otil.domain.like.controller;

import com.spartanullnull.otil.domain.like.service.LikeService;
import com.spartanullnull.otil.security.Impl.UserDetailsImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/posts")
@RequiredArgsConstructor
public class CommentLikeController {

    private final LikeService likeService;

    // 댓글 좋아요 누르기 API
    @PostMapping("/{postId}/comments/{commentId}/like")
    public ResponseEntity<String> createLikeForComment(
        @PathVariable Long postId,
        @PathVariable Long commentId,
        @AuthenticationPrincipal UserDetailsImpl userDetails) {
        likeService.createLikeForComment(userDetails.getUser().getId(), commentId);
        return ResponseEntity.ok("댓글 좋아요 성공!");
    }

    // 댓글 좋아요 취소하기 API
    @DeleteMapping("/{postId}/comments/{commentId}/like")
    public ResponseEntity<String> deleteLikeForComment(
        @PathVariable Long postId,
        @PathVariable Long commentId,
        @AuthenticationPrincipal UserDetailsImpl userDetails) {
        likeService.deleteLikeForComment(userDetails.getUser().getId(), commentId);
        return ResponseEntity.ok("댓글 좋아요 취소!");
    }

    // 댓글 좋아요 개수 조회 API
    @GetMapping("/{postId}/comments/{commentId}/count")
    public ResponseEntity<String> getLikeCountForComment(@PathVariable Long commentId) {
        Long likeCount = likeService.getLikeCountForComment(commentId);
        return ResponseEntity.ok("댓글 좋아요 : " + likeCount);
    }
}