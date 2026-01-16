package com.yun.demo.config;

import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import com.baomidou.mybatisplus.extension.plugins.PerformanceInterceptor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * Mybatis-plus config.
 */
@Configuration
@ConfigurationProperties(prefix = "mybatis-plus.extra")
@EnableTransactionManagement
@MapperScan("com.yun.demo.*.mapper")
@Slf4j
@Setter
public class MybatisPlusConfig {
    private String dialect;

    @Bean
    public PaginationInterceptor paginationInterceptor() {
        PaginationInterceptor pi = new PaginationInterceptor();
        pi.setDialectType(dialect);
        log.info("dialect: {}", dialect);
        return pi;
    }

    /**
     * sql execution performance plugin.
     */
    @Bean
    @Profile({"dev", "test", "local"}) // tip: don't active for production,
    public PerformanceInterceptor performanceInterceptor() {
        return new PerformanceInterceptor();
    }
}