package com.gxdxx.instagram.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record UserDeleteRequest(

        @Schema(description = "비밀번호", example = "123123")
        @Size(min = 4, max = 15)
        @NotBlank
        String password

) {
}