package com.studyproject.boardproject.response;

import com.studyproject.boardproject.dto.ArticleCommentDto;

import java.time.LocalDateTime;

public record ArticleCommentResponse(
        Long id,
        String content,
        String nickname,
        String email,
        LocalDateTime createdAt
) {
    public static ArticleCommentResponse of(Long id, String content, String nickname, String email, LocalDateTime createdAt) {
        return new ArticleCommentResponse(id, content, nickname, email, createdAt);
    }

    public static ArticleCommentResponse from(ArticleCommentDto dto) {
        String nickname = dto.userAccountDto().nickname();

        if (nickname == null || nickname.isBlank()) {
            nickname = dto.userAccountDto().userId();
        }

        return new ArticleCommentResponse(
                dto.id(),
                dto.content(),
                nickname,
                dto.userAccountDto().email(),
                dto.createdAt()
        );
    }
}