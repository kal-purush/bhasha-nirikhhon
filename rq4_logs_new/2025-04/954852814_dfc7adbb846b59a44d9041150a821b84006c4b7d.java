package com.example.auth_service.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

@Data
public class PasswordResetDto {
    @NotBlank(message = "Токен не может быть пустым")
    private String token;

    @NotBlank(message = "Новый пароль не может быть пустым")
    @Pattern(
        regexp = "^(?=.*[0-9])(?=.*[a-z])(?=.*[A-Z])(?=.*[!@#$%^&*()_+\\-=\\[\\]{}|;:,.<>?]).{8,}$",
        message = "Пароль должен содержать минимум 8 символов, включая заглавные и строчные буквы, цифры и специальные символы"
    )
    private String newPassword;
} 