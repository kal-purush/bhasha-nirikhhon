package com.spartanullnull.otil.post.controller;

import com.spartanullnull.otil.global.common.*;
import com.spartanullnull.otil.post.dto.*;
import com.spartanullnull.otil.post.entity.*;
import com.spartanullnull.otil.post.service.*;
import lombok.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/posts")
@RequiredArgsConstructor
public class PostController {

    private final PostService postService;

    @PostMapping
    public ResponseEntity<PostResponseDto> addPost(@RequestBody PostRequestDto requestDto) {
        PostResponseDto responseDto = postService.createPost(requestDto);
        return ResponseEntity.status(201).body(responseDto);
    }

    @GetMapping("/{postId}")
    public ResponseEntity<CommonResponseDto> getPost(@PathVariable Long postId) {
        try {
            PostResponseDto responseDto = postService.getPost(postId);
            return ResponseEntity.ok().body(responseDto);
        } catch (NullPointerException e) {
            return ResponseEntity.badRequest()
                .body(new CommonResponseDto(e.getMessage(), HttpStatus.NOT_FOUND.value()));
        }
    }

}