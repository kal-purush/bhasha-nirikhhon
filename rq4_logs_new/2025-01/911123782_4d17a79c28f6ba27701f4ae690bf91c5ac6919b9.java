package com.roomfit.be.global.config;

import com.roomfit.be.global.util.RandomCodeGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.SecureRandom;

@Configuration
public class AuthConfig {
    @Bean
    public SecureRandom secureRandom() {
        return new SecureRandom(); // SecureRandom을 빈으로 등록
    }

    @Bean
    public RandomCodeGenerator randomCodeGenerator(SecureRandom secureRandom) {
        return new RandomCodeGenerator(secureRandom); // SecureRandom을 의존성으로 주입
    }
}