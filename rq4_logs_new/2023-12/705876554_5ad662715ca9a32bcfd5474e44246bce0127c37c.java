package ru.sberbank.edu.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.client.RestTemplate;
import ru.sberbank.edu.WeatherCache;
import ru.sberbank.edu.WeatherProvider;

@Configuration
@PropertySource("classpath:app.properties")
public class MyConfig {

    @Value("${apiKey}")
    private String apiKey;

    @Bean
    public RestTemplate restTemplate(){
       RestTemplate template =  new RestTemplate();
       return template;
    }

    @Bean
    public WeatherProvider weatherProvider(){
        WeatherProvider provider =  new WeatherProvider(restTemplate());
        provider.setApiKey(apiKey);
        return provider;
    }

    @Bean
    public WeatherCache weatherCache(){
        WeatherCache cache =  new WeatherCache(weatherProvider());
        return cache;
    }

}