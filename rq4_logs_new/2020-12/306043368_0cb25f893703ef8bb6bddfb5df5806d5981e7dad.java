package org.example.config.rpc;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * RestTemplate 配置类
 *
 * @author walle
 * @version V1.0
 * @since 2020-12-10 21:48
 */
@Configuration
public class RestTemplateConfig {

    /**
     * 设置restTemplate
     *
     * @return RestTemplate
     */
    @Bean(name = "restTemplate")
    public RestTemplate createRestTemplate() {

        int oneMinute = 60 * 1000;

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(oneMinute);
        requestFactory.setReadTimeout(oneMinute);

        return new RestTemplate(requestFactory);
    }
}