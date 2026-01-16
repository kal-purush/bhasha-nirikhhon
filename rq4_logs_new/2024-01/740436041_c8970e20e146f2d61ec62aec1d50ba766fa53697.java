package com.studyproject.boardproject.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

import java.util.Optional;

@EnableJpaAuditing
@Configuration
public class JpaConfig {

    @Bean
    public AuditorAware<String> auditorAware() {
        // TODO: 스프링 시큐리티로 인증 기능을 만들때 값을 가져와서 value에 이름 값을 넣어줘야 함
        return () -> Optional.of("hyeon"); // 수정시 들어가는 이름
    }
}