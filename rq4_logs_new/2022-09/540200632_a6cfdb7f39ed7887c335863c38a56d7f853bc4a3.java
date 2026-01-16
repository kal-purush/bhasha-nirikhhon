package com.huiyuan2.cloud.mybatis.configuration;

import com.huiyuan2.cloud.mybatis.properties.MybatisProperties;
import lombok.AllArgsConstructor;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @description: mybatis plus自动装配
 * @author： 灰原二
 * @date: 2022/9/24 13:15
 */
@AllArgsConstructor
@EnableConfigurationProperties(MybatisProperties.class)
@MapperScan("com.huiyuan2.**.mapper.**")
public class MybatisPlushAutoConfiguration implements WebMvcConfigurer {

//    public S
}