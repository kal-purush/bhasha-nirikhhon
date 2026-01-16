package com.chapssal_tteok.preview.domain.resume.converter;

import com.chapssal_tteok.preview.domain.resume.dto.ResumeResponseDTO;
import com.chapssal_tteok.preview.domain.resume.entity.Resume;

public class ResumeConverter {

    public static ResumeResponseDTO.CreateResumeResultDTO toCreateResumeResultDTO(Resume resume) {
        return ResumeResponseDTO.CreateResumeResultDTO.builder()
                .resumeId(resume.getId())
                .userId(resume.getUser().getId())
                .title(resume.getTitle())
                .createdAt(resume.getCreatedAt())
                .build();
    }

    public static ResumeResponseDTO.ResumeDTO toResumeDTO(Resume resume) {
        return ResumeResponseDTO.ResumeDTO.builder()
                .resumeId(resume.getId())
                .userId(resume.getUser().getId())
                .title(resume.getTitle())
                .createdAt(resume.getCreatedAt())
                .updatedAt(resume.getUpdatedAt())
                .build();
    }
}