package com.smyr.showmeyourrecipe.controller;


import com.smyr.showmeyourrecipe.dto.CommentRequestDto;
import com.smyr.showmeyourrecipe.dto.CommentResponseDto;
import com.smyr.showmeyourrecipe.entity.CommentLike;
import com.smyr.showmeyourrecipe.entity.CommentLikeKey;
import com.smyr.showmeyourrecipe.repository.CommentLikeRepository;
import com.smyr.showmeyourrecipe.service.CommentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/post")
public class CommentController {
    private final CommentService commentService;
    private final CommentLikeRepository commentLikeRepository;

    @GetMapping("/{postId}/comments")
    public List<CommentResponseDto> getComment(@PathVariable Long postId) {
        return commentService.getComment(postId);
    }

    @PostMapping("/{postId}/comments")
    public CommentResponseDto createComment(@PathVariable Long postId, @RequestBody CommentRequestDto requestDto) {
        return commentService.createComment(postId, requestDto);
    }

    @PatchMapping("/{postId}/comments/{commentId}")
    public CommentResponseDto updateComment(@PathVariable Long postId, @PathVariable Long commentId, @RequestBody CommentRequestDto requestDto) {
        return commentService.updateComment(postId, commentId, requestDto);
    }

    @DeleteMapping("/{postId}/comments/{commentId}")
    public CommentResponseDto deleteComment(@PathVariable Long postId, @PathVariable Long commentId) {
        return commentService.deleteComment(postId, commentId);
    }
//    @DeleteMapping("/{postId}/comments/{commentId}")
//    public void deleteComment(@PathVariable Long postId, @PathVariable Long commentId) {
//        commentService.deleteComment(postId, commentId);
//    }

    @PostMapping("/{postId}/comments/{commentId}/like")
    public CommentResponseDto likeComment(@PathVariable Long postId, @PathVariable Long commentId) {
        return commentService.likeComment(postId, commentId);
    }

    @DeleteMapping("/{postId}/comments/{commentId}/like")
    public CommentResponseDto deleteLikeComment(@PathVariable Long postId, @PathVariable Long commentId) {
        return commentService.deleteLikeComment(postId, commentId);
    }

    //
//    @GetMapping("/like/{userid}/{commentid}")
//    public CommentLike get(@PathVariable Long userid,@PathVariable Long commentid){
//        CommentLikeKey key = new CommentLikeKey(userid,commentid);
//        Optional<CommentLike> opt =
//                commentLikeRepository.findById(key);
//
//        return opt.orElseGet(CommentLike::new);
//    }
}