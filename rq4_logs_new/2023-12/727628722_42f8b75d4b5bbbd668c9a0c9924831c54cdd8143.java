package com.spartanullnull.otil.comment.dto;

import com.spartanullnull.otil.comment.entity.Comment;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CommentResponseDto { // 클라이언트에게 응답할 때 반환하는 데이터를 정의
    private Long id;
    private String commentText;
    private Long userId; // 사용자 ID
    private String username; // 사용자 이름

    private Long postId; // 게시물 ID

    // 생성자
    public CommentResponseDto(Long id, String commentText, Long userId, String username,
        Long postId) {
        this.id = id;
        this.commentText = commentText;
        this.userId = userId;
        this.username = username;
        this.postId = postId;
    }

    // Entity에서 Dto로 변환하는 메서드
    public static CommentResponseDto fromEntity(Comment comment) {
        return new CommentResponseDto(
            comment.getId(),
            comment.getCommentText(),
            comment.getUser().getId(),
            comment.getUser().getUsername(),
            comment.getPost().getId()
        );
    }
}