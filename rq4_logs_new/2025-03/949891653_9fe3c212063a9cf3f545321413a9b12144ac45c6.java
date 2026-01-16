package com.chapssal_tteok.preview.domain.user.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

public class UserRequestDTO {

    @Getter
    @Builder
    @AllArgsConstructor
    public static class CreateUserDTO {
        @NotBlank(message = "username은 필수 입력 값입니다.")
        @Size(min = 2, max = 20, message = "username은 2~20자 사이여야 합니다.")
        private String username;

        @NotBlank(message = "이름은 필수 입력 값입니다.")
        @Size(min = 2, max = 20, message = "이름은 2~20자 사이여야 합니다.")
        private String name;

        @NotBlank(message = "이메일은 필수 입력 값입니다.")
        @Email(message = "올바른 이메일 형식이어야 합니다.")
        private String email;

        @NotBlank(message = "비밀번호는 필수 입력 값입니다.")
        @Size(min = 8, message = "비밀번호는 최소 8자리 이상이어야 합니다.")
        private String password;
    }

    @Getter
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL) // null 값은 JSON 변환 시 제외
    public static class UpdateUserDTO {

        @Size(min = 2, max = 20, message = "이름은 2~20자 사이여야 합니다.")
        private String name;

        @NotBlank(message = "현재 비밀번호는 필수 입력 값입니다.")
        private String currentPassword;

        @Size(min = 8, message = "비밀번호는 최소 8자리 이상이어야 합니다.")
        private String password;
    }
}