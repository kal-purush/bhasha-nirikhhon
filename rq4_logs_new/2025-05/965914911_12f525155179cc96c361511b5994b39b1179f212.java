package com.hanyahunya.gitRemind.token.service;

import com.hanyahunya.gitRemind.infrastructure.email.SendEmailService;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class SecurityAlertEmailService {
    private final SendEmailService sendEmailService;

    public void sendCookieHijackingAlert(String email, String gitUsername) {
        if (gitUsername == null) {
            gitUsername = "사용자";
        }
        Map<String, Object> params = new HashMap<>();
        params.put("name", gitUsername);
        sendEmailService.sendEmail(email, "보안 알림", "accountAlert", params);
    }
}