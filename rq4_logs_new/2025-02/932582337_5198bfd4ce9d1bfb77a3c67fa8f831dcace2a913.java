package com.j30n.stoblyx.adapter.in.web;

import com.j30n.stoblyx.common.dto.ApiResponse;
import com.j30n.stoblyx.common.security.JwtProvider;
import com.j30n.stoblyx.domain.model.user.User;
import com.j30n.stoblyx.port.in.UserUseCase;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 인증 관련 API를 처리하는 컨트롤러
 * 회원가입과 로그인 기능을 제공합니다.
 */
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final UserUseCase userUseCase;
    private final JwtProvider jwtProvider;

    /**
     * 새로운 사용자를 등록합니다.
     *
     * @param request 사용자 등록 정보 (이메일, 비밀번호, 이름)
     * @return 등록된 사용자 정보
     */
    @PostMapping("/register")
    public ResponseEntity<ApiResponse<RegisterResponse>> register(
        @Valid @RequestBody RegisterRequest request) {
        try {
            User user = userUseCase.registerUser(
                request.email(),
                request.password(),
                request.name()
            );

            RegisterResponse response = new RegisterResponse(
                user.getId(),
                user.getEmail(),
                user.getName()
            );

            return ResponseEntity.ok(ApiResponse.success(
                "회원가입이 완료되었습니다.",
                response
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("회원가입 처리 중 오류가 발생했습니다."));
        }
    }

    /**
     * 사용자 로그인을 처리합니다.
     *
     * @param request 로그인 정보 (이메일, 비밀번호)
     * @return JWT 토큰
     */
    @PostMapping("/login")
    public ResponseEntity<ApiResponse<LoginResponse>> login(
        @Valid @RequestBody LoginRequest request) {
        try {
            User user = userUseCase.findUserByEmail(request.email());
            userUseCase.validateUser(request.email(), request.password());

            String token = jwtProvider.createToken(user.getId());
            LoginResponse response = new LoginResponse(token);

            return ResponseEntity.ok(ApiResponse.success(
                "로그인이 완료되었습니다.",
                response
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                .body(ApiResponse.error(e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                .body(ApiResponse.error("로그인 처리 중 오류가 발생했습니다."));
        }
    }

    // Request/Response Records
    record RegisterRequest(
        @NotBlank(message = "이메일은 필수입니다")
        @Email(message = "올바른 이메일 형식이어야 합니다")
        String email,

        @NotBlank(message = "비밀번호는 필수입니다")
        @Size(min = 8, max = 100, message = "비밀번호는 8자 이상 100자 이하여야 합니다")
        String password,

        @NotBlank(message = "이름은 필수입니다")
        @Size(min = 2, max = 50, message = "이름은 2자 이상 50자 이하여야 합니다")
        String name
    ) {
    }

    record RegisterResponse(
        Long id,
        String email,
        String name
    ) {
    }

    record LoginRequest(
        @NotBlank(message = "이메일은 필수입니다")
        @Email(message = "올바른 이메일 형식이어야 합니다")
        String email,

        @NotBlank(message = "비밀번호는 필수입니다")
        String password
    ) {
    }

    record LoginResponse(
        String token
    ) {
    }
} 