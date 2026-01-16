package com.example.sideproject.domain.comment.dto;

import com.example.sideproject.domain.comment.entity.Comment;
import com.example.sideproject.domain.project.entity.Project;
import com.example.sideproject.domain.user.entity.User;
import jakarta.validation.constraints.NotBlank;

public record CommentRequestDto(
        Long parentId,
        @NotBlank(message = "댓글 내용은 필수 기입 항목입니다.") String content
) {

    public Comment toEntity(User user, Project project) {
        return Comment.builder()
                .content(content)
                .parentId(parentId)
                .user(user)
                .project(project)
                .build();
    }
}