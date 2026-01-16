package com.xianjinxia;

import com.didispace.swagger.EnableSwagger2Doc;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;

@EnableSwagger2Doc
@SpringBootApplication
//去除数据源的自动化配置
//@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
public class Springboot01Application extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		// 注意这里要指向原先用main方法执行的Application启动类
		return builder.sources(Springboot01Application.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Springboot01Application.class, args);
	}
}