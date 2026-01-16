package com.cartas.jaktani.config;

import static springfox.documentation.builders.PathSelectors.regex;
import java.util.ArrayList;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.ParameterBuilder;
import springfox.documentation.schema.ModelRef;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Parameter;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

 
@Configuration
@EnableSwagger2
public class SwaggerConfig {
	
	    @Bean
	    public Docket policyApi() {
            ParameterBuilder tokenHeaderBuilder = new ParameterBuilder();
            tokenHeaderBuilder.name("Authorization")                 // name of header
						     .modelRef(new ModelRef("string"))
						     .parameterType("header")               // type - header
						     .defaultValue("")
						     .required(false)                // for compulsory
						     .build();
            
            ParameterBuilder agentHeaderBuilder = new ParameterBuilder();
            agentHeaderBuilder.name("username")                 // name of header
							 .modelRef(new ModelRef("string"))
							 .parameterType("header")               // type - header
							 .defaultValue("")
							 .required(false)                // for compulsory
							 .build();
            
            ParameterBuilder deviceHeaderBuilder = new ParameterBuilder();
            deviceHeaderBuilder.name("deviceId")                 // name of header
						     .modelRef(new ModelRef("string"))
						     .parameterType("header")               // type - header
						     .defaultValue("")
						     .required(false)                // for compulsory
						     .build();
            
		    java.util.List<Parameter> aParameters = new ArrayList<>();
		    aParameters.add(tokenHeaderBuilder.build());
		    aParameters.add(agentHeaderBuilder.build());
		    aParameters.add(deviceHeaderBuilder.build());
		    
            return new Docket(DocumentationType.SWAGGER_2)
                    .pathMapping("/")
                    .select()
                    .paths(postPaths()).build()
                    .apiInfo(apiInfo())
                    .globalOperationParameters(aParameters);
			
	    }
	    
        private Predicate<String> postPaths() {
		    return Predicates.or(regex("/authentication.*"),
		    		Predicates.or(regex("/token.*"),
				    Predicates.or(regex("/api.*")
		    		))
		    		);
		}

        @Bean
        public ApiInfo apiInfo() {
            final ApiInfoBuilder builder = new ApiInfoBuilder();
            builder.title("Jak Tani Indonesia")
            .description("List of all the APIs of Jak Tani Application ");
            return builder.build();
        }
}