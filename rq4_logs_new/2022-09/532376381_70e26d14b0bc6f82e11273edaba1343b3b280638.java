package com.example.sbemintic.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Collections;

@Configuration
@EnableSwagger2
public class SpringFoxConfig {

    @Value("${api.swagger.base_package}")
    private String basePackage;
    @Value("${api.swagger.base.title}")
    private String tittle;
    @Value("${api.swagger.base.description}")
    private String description;
    @Value("${api.swagger.version}")
    private String version;
    @Value("${api.swagger.terms}")
    private String terms;
    @Value("${api.swagger.contact.name}")
    private String contactName;
    @Value("${api.swagger.contact.email}")
    private String contactEmail;
    @Value("${api.swagger.contact.url}")
    private String contactUrl;
    @Value("${api.swagger.license.name}")
    private String licenseName;
    @Value("${api.swagger.license.url}")
    private String licenseUrl;

    @Bean
    public Docket api(){
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(getApiInfo()).securityContexts(Collections.emptyList())
                .securitySchemes(Collections.emptyList()).select().apis(RequestHandlerSelectors.basePackage(basePackage)).paths(PathSelectors.any())
                .build();
    }

    private ApiInfo getApiInfo() {
        return new ApiInfo(tittle,description,version,terms, new Contact(contactName,contactUrl,contactEmail),licenseName,licenseUrl,
                Collections.emptyList());
    }

}