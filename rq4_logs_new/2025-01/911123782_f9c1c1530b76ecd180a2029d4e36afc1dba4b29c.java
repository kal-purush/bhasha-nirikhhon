package com.roomfit.be.global.util;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;

@RequiredArgsConstructor
@Component
public class RandomCodeGenerator {
    private final SecureRandom secureRandom;

    public String generateSecureRandomCode(int length) {
        StringBuilder code = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int digit = secureRandom.nextInt(10);
            code.append(digit);
        }
        return code.toString();
    }
}
