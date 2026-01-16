package com.roomfit.be.auth.application;

import com.roomfit.be.email.EmailSendEvent;
import com.roomfit.be.redis.application.ReactiveRedisService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;

@Service
@RequiredArgsConstructor
public class AuthServiceImpl implements AuthService{
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ReactiveRedisService reactiveRedisService;

    @Async("taskExecutor")
    @Override
    public void generateVerificationCode(String sessionId, String email) {

        String code = generateRandomCode(5);
        reactiveRedisService.saveWithTTL(sessionId, code, 300)
                .doOnTerminate(() -> {
                    String subject = "Verification Code";
                    String body = "Here is your verification code: " + code;
                    applicationEventPublisher.publishEvent(new EmailSendEvent(email, subject, body));
                })
                .subscribe();
    }

    @Override
    public boolean verifyVerificationCode(String sessionId, String code) {
        /**
         * 동기적으로
         */
        String savedCode = (String) reactiveRedisService.get(sessionId).block(); // block()을 사용하여 동기적으로 값 가져오기
        return savedCode != null && savedCode.equals(code);
    }

    private String generateRandomCode(int length) {
        SecureRandom random = new SecureRandom();
        StringBuilder code = new StringBuilder();

        for (int i = 0; i < length; i++) {
            int digit = random.nextInt(10); // 0~9 사이의 숫자 생성
            code.append(digit);
        }

        return code.toString();
    }
}