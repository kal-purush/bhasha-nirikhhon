package com.DEMO.backend.member.application.dto;

public record SignUpRequest(
        String email,
        String password,
        String nickname
) {
}