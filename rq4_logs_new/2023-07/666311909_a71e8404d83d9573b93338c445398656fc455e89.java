package com.sparta.communityback.controller;

import com.sparta.communityback.dto.CommentRequestDto;
import com.sparta.communityback.dto.CommentResponseDto;
import com.sparta.communityback.dto.StatusResponseDto;
import com.sparta.communityback.service.CommentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/boards/{boardId}/posts/{postId}/comments")
@RequiredArgsConstructor
public class CommentController {

    private final CommentService commentService;

//    @PostMapping()
//    public ResponseEntity<StatusResponseDto> createComment(@PathVariable Long boardId,
//                                                           @PathVariable Long postId,
//                                                           // 아직 없음
//                                                           //@AuthenticationPrincipal UserDetailsImpl userDetails,
//                                                           @RequestBody CommentRequestDto requestDto) {
//        return new ResponseEntity<>(commentService.createComment(boardId, postId, requestDto), HttpStatus.OK);
//    }
}