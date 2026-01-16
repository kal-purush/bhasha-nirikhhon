package com.example.sideproject.domain.chat.dto;

import com.example.sideproject.domain.chat.entity.ChatRoomType;
import com.example.sideproject.domain.pr.entity.PublicResumes;
import com.example.sideproject.domain.project.entity.Project;

public record ContentSummaryResponse(
        Long id,
        String title,
        ChatRoomType type
) {
    // 단일 팩토리 메서드로 통합
    public static ContentSummaryResponse from(Object content) {
        if (content instanceof Project project) {
            return new ContentSummaryResponse(
                    project.getId(),
                    project.getTitle(),
                    ChatRoomType.PROJECT
            );
        } else if (content instanceof PublicResumes pr) {
            return new ContentSummaryResponse(
                    pr.getId(),
                    pr.getResume().getTitle(),
                    ChatRoomType.PR
            );
        }
        throw new IllegalArgumentException("Unsupported content type: " +
                (content != null ? content.getClass().getName() : "null"));
    }
}