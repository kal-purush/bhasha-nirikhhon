package com.smyr.showmeyourrecipe.controller;

import com.smyr.showmeyourrecipe.dto.post.PostRequest;
import com.smyr.showmeyourrecipe.service.PostService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/post")
public class PostController {
    private final PostService postService;

    @PostMapping("")
    public void createPost(@AuthenticationPrincipal UserDetails userDetails,
                           @RequestBody PostRequest request) {
//        postService.createPost(userDetails.getUser(), request);
    }

    @PatchMapping("/{postId}")
    public void updatePost(@AuthenticationPrincipal UserDetails userDetails,
                           @RequestBody PostRequest request, @PathVariable("postId") Long postId) {
//        postService.updatePost(userDetails.getUser(), request, postId);
    }

    @DeleteMapping("/{postId}")
    public void deletePost(@AuthenticationPrincipal UserDetails userDetails,
                           @PathVariable("postId") Long postId) {
//        postService.deletePost(userDetails.getUser(), postId);
    }
}