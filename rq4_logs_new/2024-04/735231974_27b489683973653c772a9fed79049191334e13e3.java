package com.bodytok.healthdiary.dto.comment;

import com.bodytok.healthdiary.domain.Comment;
import com.bodytok.healthdiary.dto.diary.DiaryDto;
import com.bodytok.healthdiary.dto.diary.response.DiaryResponse;

import java.time.LocalDateTime;

public record CommentWithDiaryResponse(
        Long id,
        String content,
        Long userId,
        String userEmail,
        DiaryResponse diary,
        LocalDateTime createdAt,
        LocalDateTime modifiedAt
) {

    public static CommentWithDiaryResponse from(Comment entity) {
        return new CommentWithDiaryResponse(
                entity.getId(),
                entity.getContent(),
                entity.getUserAccount().getId(),
                entity.getUserAccount().getEmail(),
                DiaryResponse.from(
                        DiaryDto.from(entity.getPersonalExerciseDiary())
                ),
                entity.getCreatedAt(),
                entity.getModifiedAt()
        );
    }
}