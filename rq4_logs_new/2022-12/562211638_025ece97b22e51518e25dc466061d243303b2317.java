package ru.vasili_zlobin.hibernate.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.vasili_zlobin.hibernate.controllers.CustomerDao;
import ru.vasili_zlobin.hibernate.controllers.ProductDao;

@Configuration
public class SpringContextConfig {

    @Bean
    public ProductDao productDao() {
        return new ProductDao();
    }

    @Bean
    public CustomerDao customerDao() {
        return new CustomerDao();
    }
}