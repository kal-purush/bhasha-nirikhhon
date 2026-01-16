package faang.school.postservice.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI customOpenApi() {
        return new OpenAPI()
                .info( new Info()
                        .title("Post service API")
                        .version("0.1.0.1")
                        .description("Post service gives the ability to write text posts to share your thoughts, " +
                                "knowledge and information with other users."));
    }
}