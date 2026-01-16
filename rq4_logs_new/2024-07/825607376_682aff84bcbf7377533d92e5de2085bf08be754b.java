package refooding.api.common.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {
    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(getApiInfo());
    }

    private Info getApiInfo(){
        return new Info()
                .title("리푸딩 API 문서")
                .version("1.0.0");
    }
}