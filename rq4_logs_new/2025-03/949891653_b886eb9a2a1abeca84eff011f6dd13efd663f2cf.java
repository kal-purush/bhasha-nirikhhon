package com.chapssal_tteok.preview.domain.resume.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

public class ResumeRequestDTO {

    @Getter
    public static class CreateResumeDTO {

        @NotNull(message = "사용자 id는 필수 입력 값입니다.")
        private Long user;

        @NotBlank(message = "제목은 필수 입력 값입니다.")
        private String title;
    }

    @Getter
    public static class UpdateResumeDTO {

        private String title;
    }
}