package com.openlabs.shoppingmall.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration    // 스프링 실행시 설정파일 어노테이션
@EnableSwagger2    // Swagger2를 사용하겠다는 어노테이션
public class SwaggerConfig {

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("ShoppingMall")
                .description("shopping mall Example")
                .build();
    }

    //swagger 설정.
    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.openlabs.shoppingmall.controller"))
                .paths(PathSelectors.ant("/labshop/v1/**"))
                .build();
    }
}