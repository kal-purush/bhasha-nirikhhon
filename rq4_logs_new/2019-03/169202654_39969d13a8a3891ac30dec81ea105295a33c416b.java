package com.gcp.vision.api.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.gcp.vision.api.util.ImageSearchConstants;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;


/**
 * @author Anuj Kumar
 * 
 *   This class is to have swagger configuration
 */
@Configuration
@EnableSwagger2
public class SwaggerConfig {

	@Value(ImageSearchConstants.SWAGGER_INFO)
	private String swaggerInfo;
	
	@Value(ImageSearchConstants.SWAGGER_BASE_PACKAGE)
	private String basePackage;

	@Bean
	public Docket productApi() {
		Docket docket = new Docket(DocumentationType.SWAGGER_2)
				.select()
				.apis(RequestHandlerSelectors.basePackage(basePackage))
				.build();
		docket.useDefaultResponseMessages(false);
		return docket;

	}
}