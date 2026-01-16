package com.spartanullnull.otil.comment.controller;

import com.spartanullnull.otil.comment.dto.CommentRequestDto;
import com.spartanullnull.otil.comment.dto.CommentResponseDto;
import com.spartanullnull.otil.comment.service.CommentService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/posts/{postId}/comments")
public class CommentController {

    private final CommentService commentService;

    @Autowired
    public CommentController(CommentService commentService) {
        this.commentService = commentService;
    }

    // 댓글 작성
    // postId       TIL Id
    // userId       사용자 Id
    // requestDto   댓글 작성 요청 정보
    // return       ResponseEntity<CommentResponseDto>
    //              작성된 댓글과 HTTP 상태 코드 반환
    @PostMapping
    public ResponseEntity<CommentResponseDto> createComment(
        @PathVariable Long postId,
        @RequestParam Long userId,
        @RequestBody CommentRequestDto requestDto
    ) {
        CommentResponseDto createdComment = commentService.createComment(postId, userId, requestDto);
        return new ResponseEntity<>(createdComment, HttpStatus.CREATED);
    }

    // 특정 게시물의 댓글 목록 조회
    // postId       TIL Id
    // return       ResponseEntity<List<CommentResponseDto>>
    //              댓글 목록과 HTTP 상태 코드 반환
    @GetMapping
    public ResponseEntity<List<CommentResponseDto>> getComments(@PathVariable Long postId) {
        List<CommentResponseDto> comments = commentService.getCommentsByPostId(postId);
        return new ResponseEntity<>(comments, HttpStatus.OK);
    }

    // 댓글 수정
    // postId       TIL Id
    // commentId    댓글 Id
    // requestDto   수정할 댓글 정보
    // return       ResponseEntity<CommentResponseDto>
    //              수정된 댓글과 HTTP 상태 코드 반환
    @PatchMapping("/{commentId}")
    public ResponseEntity<CommentResponseDto> updateComment(
        @PathVariable Long postId,
        @PathVariable Long commentId,
        @RequestBody CommentRequestDto requestDto
    ) {
        CommentResponseDto updatedComment = commentService.updateComment(commentId, requestDto);
        return new ResponseEntity<>(updatedComment, HttpStatus.OK);
    }

    // 댓글 삭제
    // postId       TIL Id
    // commentId    댓글 Id
    // return       ResponseEntity<Void> HTTP 상태 코드 반환
    @DeleteMapping("/{commentId}")
    public ResponseEntity<Void> deleteComment(
        @PathVariable Long postId,
        @PathVariable Long commentId
    ) {
        commentService.deleteComment(commentId);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}