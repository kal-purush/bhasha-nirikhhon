package com.team6.config;/**
 * Created by VLoye on 2018/4/28.
 */

import org.springframework.context.annotation.Bean;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author VLoye
 * @ClassName SwaggerConfig
 * @Description Swagger配置文件
 * @Date 16:57  2018/4/28
 * @Version 1.0
 **/

@WebAppConfiguration
@EnableSwagger2
@EnableWebMvc
public class SwaggerConfig{

    @Bean
    public Docket api(){
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.any())
                .build()
                .apiInfo(apiInfo());
    }

    private ApiInfo apiInfo(){

        return new ApiInfoBuilder()
                .title("XX项目接口文档")
                .description("接口文档测试")
                .version("1.0")
                .license("V")
                .licenseUrl("")
                .build();
    }

}