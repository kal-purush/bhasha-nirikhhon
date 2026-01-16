package com.spartanullnull.otil.domain.recommend.controller;

import com.spartanullnull.otil.domain.recommend.service.RecommendService;
import com.spartanullnull.otil.security.Impl.UserDetailsImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/posts")
@RequiredArgsConstructor
public class RecommendController {

    private final RecommendService recommendService;

    // 게시물 추천하기 API
    @PostMapping("/{postId}/recommend")
    public ResponseEntity<String> createRecommend(
        @PathVariable Long postId,
        @AuthenticationPrincipal UserDetailsImpl userDetails) {

        // 로그인한 사용자 체크
        if (userDetails == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                "로그인한 사용자만 게시물 추천을 누를 수 있습니다.");
        }

        recommendService.createRecommend(userDetails.getUser().getId(), postId);
        return ResponseEntity.ok("게시물 추천 성공");
    }

    // 게시물 추천 취소하기 API
    @DeleteMapping("/{postId}/recommend")
    public ResponseEntity<String> deleteRecommend(
        @PathVariable Long postId,
        @AuthenticationPrincipal UserDetailsImpl userDetails) {

        // 로그인한 사용자 체크
        if (userDetails == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                "로그인한 사용자만 게시물 추천을 취소할 수 있습니다.");
        }

        recommendService.deleteRecommend(userDetails.getUser().getId(), postId);
        return ResponseEntity.ok("게시물 추천 취소!");
    }

    // 게시물 추천 개수 조회 API
    @GetMapping("/{postId}/recommend/count")
    public ResponseEntity<String> getRecommendCount(@PathVariable Long postId) {
        Long recommendCount = recommendService.getRecommendCount(postId);
        return ResponseEntity.ok("게시물 추천 수 : " + recommendCount);
    }
}