package com.roomfit.be.auth.presentation;

import com.roomfit.be.auth.application.AuthService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
@Slf4j
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    @PostMapping("/login")
    void login() {
    }

    /**
     * 이미 가입된 이메일은 접근이 불가하게
     */
    @PostMapping("/code")
    String sendVerificationCode(@RequestBody String email, HttpServletRequest request) {
        log.info(email);
        HttpSession session = request.getSession();
        String sessionId = session.getId();
        log.info(session.getId());

        authService.generateVerificationCode(sessionId, email);
        return "successfully";
    }

    @PostMapping("/verify")
    boolean verifyVerificationCode(@RequestBody String code, HttpServletRequest request) {
        HttpSession session = request.getSession();
        String sessionId = session.getId();
        return authService.verifyVerificationCode(sessionId, code);

    }
}