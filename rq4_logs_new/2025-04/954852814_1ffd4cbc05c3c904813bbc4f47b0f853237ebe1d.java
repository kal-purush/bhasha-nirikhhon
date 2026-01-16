package com.example.auth_service.auth;

import com.example.auth_service.controller.AuthController;
import com.example.auth_service.dto.AuthResponse;
import com.example.auth_service.dto.EmailVerificationDto;
import com.example.auth_service.dto.UserSigninDto;
import com.example.auth_service.dto.UserSignupDto;
import com.example.auth_service.exception.InvalidConfirmationCodeException;
import com.example.auth_service.exception.UserNotActivatedException;
import com.example.auth_service.exception.UserNotFoundException;
import com.example.auth_service.service.AuthService;
import jakarta.mail.MessagingException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.BadCredentialsException;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@AutoConfigureMockMvc
class AuthControllerTest {

    @Mock
    private AuthService authService;

    @Mock
    private HttpServletResponse response;

    @InjectMocks
    private AuthController authController;

    @Autowired
    private Validator validator;

    // =======================
    // Тесты регистрации
    // =======================

    @Test
    @DisplayName("POST /auth/register-user - Успешная регистрация пользователя")
    void registerUser_Success() throws MessagingException {
        UserSignupDto userSignupDto = new UserSignupDto("user", "user@example.com", "password");

        assertDoesNotThrow(() -> authController.register(userSignupDto));
        verify(authService, times(1)).register(userSignupDto, false);
    }

    @Test
    @DisplayName("POST /auth/register-admin - Успешная регистрация администратора")
    void registerAdmin_Success() throws MessagingException {
        UserSignupDto userSignupDto = new UserSignupDto("admin", "admin@example.com", "password");

        assertDoesNotThrow(() -> authController.registerAdmin(userSignupDto));
        verify(authService, times(1)).register(userSignupDto, true);
    }

    // =======================
    // Тесты аутентификации
    // =======================

    @Test
    @DisplayName("POST /auth/login - Успешная аутентификация")
    void login_Success() {
        UserSigninDto userSigninDto = new UserSigninDto("user", "password");
        AuthResponse authResponse = new AuthResponse("jwt-token");

        when(authService.login(userSigninDto)).thenReturn(authResponse);

        ResponseEntity<AuthResponse> response = authController.login(userSigninDto, this.response);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(authResponse, response.getBody());
        verify(authService, times(1)).addJwtToCookie(any(), any());
    }

    @Test
    @DisplayName("POST /auth/login - Ошибка: пользователь не найден")
    void login_UserNotFound() {
        UserSigninDto userSigninDto = new UserSigninDto("unknownUser", "password");

        when(authService.login(userSigninDto)).thenThrow(new UserNotFoundException("User not found"));

        assertThrows(UserNotFoundException.class, () -> authController.login(userSigninDto, response));
    }

    @Test
    @DisplayName("POST /auth/login - Ошибка: неверный пароль")
    void login_BadCredentials() {
        UserSigninDto userSigninDto = new UserSigninDto("user", "wrong-password");

        when(authService.login(userSigninDto)).thenThrow(new BadCredentialsException("Bad credentials"));

        assertThrows(BadCredentialsException.class, () -> authController.login(userSigninDto, response));
    }

    @Test
    @DisplayName("POST /auth/login - Ошибка: пользователь не активирован")
    void login_UserNotActivated() {
        UserSigninDto userSigninDto = new UserSigninDto("user", "password");

        when(authService.login(userSigninDto)).thenThrow(new UserNotActivatedException("User not activated"));

        assertThrows(UserNotActivatedException.class, () -> authController.login(userSigninDto, response));
    }

    // =======================
    // Тесты подтверждения email
    // =======================

    @Test
    @DisplayName("POST /auth/verify-email - Успешное подтверждение email")
    void verifyEmail_Success() {
        EmailVerificationDto dto = new EmailVerificationDto("user@example.com", "123456");

        assertDoesNotThrow(() -> authController.verifyEmail(dto));
        verify(authService, times(1)).confirmEmail(dto.getEmail(), dto.getCode());
    }

    @Test
    @DisplayName("POST /auth/verify-email - Ошибка: неверный код подтверждения")
    void verifyEmail_InvalidCode() {
        EmailVerificationDto dto = new EmailVerificationDto("user@example.com", "wrong-code");

        doThrow(new InvalidConfirmationCodeException("Invalid code"))
                .when(authService).confirmEmail(dto.getEmail(), dto.getCode());

        assertThrows(InvalidConfirmationCodeException.class, () -> authController.verifyEmail(dto));
    }

    // =======================
    // Тесты на валидацию
    // =======================

    @Test
    @DisplayName("POST /auth/register-user - Ошибка: невалидные данные")
    void registerUser_InvalidData() {
        UserSignupDto invalidDto = new UserSignupDto("", "invalid-email", "");

        Set<ConstraintViolation<UserSignupDto>> violations = validator.validate(invalidDto);

        assertFalse(violations.isEmpty());
    }

    @Test
    @DisplayName("POST /auth/logout - Успешный выход из системы")
    void logout_Success() {
        String authHeader = "Bearer jwt-token";

        assertDoesNotThrow(() -> authController.logout(authHeader));
        verify(authService, times(1)).logout(authHeader);
    }
}