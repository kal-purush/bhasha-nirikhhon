package com.studyproject.boardproject.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.thymeleaf.spring6.templateresolver.SpringResourceTemplateResolver;

// 이 스프링 부트 설정은 사용자 정의 프로퍼티 spring.thymeleaf3.decoupled-logic을
// 사용하여 타임리프 3의 템플릿 로직 분리(decoupled logic) 기능을 이용할 수 있게 해줍니다.
// Why? -> index를 순수 마크업 상태로 유지시키는 방법이기 때문에
@Configuration
public class ThymeleafConfig {

    @Bean
    public SpringResourceTemplateResolver thymeleafTemplateResolver(
            SpringResourceTemplateResolver defaultTemplateResolver,
            Thymeleaf3Properties thymeleaf3Properties
    ) {
        defaultTemplateResolver.setUseDecoupledLogic(thymeleaf3Properties.decoupledLogic());

        return defaultTemplateResolver;
    }


    @ConfigurationProperties("spring.thymeleaf3")
    public record Thymeleaf3Properties(boolean decoupledLogic) {}
}