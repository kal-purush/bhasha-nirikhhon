package practice.mvcstarter.web.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import practice.mvcstarter.web.interceptor.AccessLogInterceptor;

/**
 * Created by Yoo Ju Jin(jujin1324@daum.net)
 * Created Date : 2022/05/25
 */

@Configuration
@RequiredArgsConstructor
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new AccessLogInterceptor())
                .order(1)
                .addPathPatterns("/**");
    }
}