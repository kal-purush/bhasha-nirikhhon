package me.zonglun.seckilling;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;

/**
 * @author : Administrator
 * @create 2018-05-06 20:08
 */
@SpringBootApplication
public class SpringBootStartApplication extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        // 注意这里要指向原先用main方法执行的Application启动类
        return builder.sources(SpringBootStartApplication.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootStartApplication.class, args);
    }
}