package com.heylhh.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig  {

    @Bean
    public Docket buildDocket() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(buildApiInfo()) // .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.heylhh"))// 要注释的接口名
                .paths(PathSelectors.any())
                .build();
    }
    private ApiInfo buildApiInfo() {
        return new ApiInfoBuilder()
                .title("API接口文档")
                .description("考勤系统")
                .contact("heylhh")
                .version("1.0.0")
                .build();
    }
}