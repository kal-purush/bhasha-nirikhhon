package com.egorbarinov.springlevelone;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan("com.egorbarinov.springlevelone")
public class AppConfig {
    @Bean(name = "magazine")
    public Magazine magazine() {
        return new RealProjectile();
    }

    @Bean(name = "weapon")
    public Weapon weapon(Magazine magazine){
        return new Kalashnikov(magazine);
    }

}