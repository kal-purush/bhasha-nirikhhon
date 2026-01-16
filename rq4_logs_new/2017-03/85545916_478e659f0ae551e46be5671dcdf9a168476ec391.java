package com.ordinov.springboot.conf;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
@EnableWebMvc
@ComponentScan("com.ordinov")
public class SpringMVCConfig extends WebMvcConfigurerAdapter{

//	@Bean
//	public InternalResourceViewResolver viewResolver(){
//		InternalResourceViewResolver resolver = new InternalResourceViewResolver();
//		resolver.setPrefix("/WEB-INF/views/");
//		resolver.setSuffix(".jsp");
//		resolver.setViewClass(JstlView.class);
//		return resolver;
//	}

//	@Override
//	public void addResourceHandlers(ResourceHandlerRegistry registry) {
//		registry.addResourceHandler("/css/**").addResourceLocations("/css/");
//		registry.addResourceHandler("/images/**").addResourceLocations("/images/");
//		registry.addResourceHandler("/js/**").addResourceLocations("/js/");
//		registry.addResourceHandler("/fonts/**").addResourceLocations("/fonts/");
//	}
	
	
}