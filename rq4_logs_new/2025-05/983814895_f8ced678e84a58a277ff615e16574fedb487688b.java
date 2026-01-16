package com.example.userservice.controller.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class UserRequest {

    @Getter
    @NoArgsConstructor
    public static class CreateUserRequest {
        @NotNull(message = "Email cannot be null")
        @Size(min = 3, max = 255, message = "Email must be between 3 and 255 characters")
        @Email
        private String email;

        @NotNull(message = "Name cannot be null")
        @Size(min = 2, max = 50, message = "Name must be between 2 and 50 characters")
        private String name;

        @NotNull(message = "Password cannot be null")
        @Size(min = 8, max = 20, message = "Password must be between 8 and 20 characters")
        private String password;
    }

}