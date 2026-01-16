package com.j30n.stoblyx.application.usecase.user.port;

/**
 * 사용자 로그인을 위한 유스케이스
 */
public interface LoginUserUseCase {
    LoginUserResponse login(LoginUserCommand command);

    record LoginUserCommand(
        String email,
        String password
    ) {
        public LoginUserCommand {
            if (email == null || email.isBlank()) {
                throw new IllegalArgumentException("이메일은 필수입니다.");
            }
            if (password == null || password.isBlank()) {
                throw new IllegalArgumentException("비밀번호는 필수입니다.");
            }
        }
    }

    record LoginUserResponse(
        Long userId,
        String email,
        String name,
        String accessToken,
        String refreshToken
    ) {
    }
} 