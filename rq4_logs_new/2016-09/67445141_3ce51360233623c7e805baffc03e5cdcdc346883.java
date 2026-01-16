package com.Polodz;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import com.Polodz.View.MainWindow;
import com.Polodz.controller.MainController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@Configuration
//@EnableAutoConfiguration//(exclude = WebMvcAutoConfiguration.class)
@ImportResource("file:src/Grown-mainContext.xml")
public class GrownApplication {

	public static void main(String[] args) {
		//SpringApplication.run();
		//Banner.Mode bannerMode= new Banner.Mode(OFF);
		new SpringApplicationBuilder(GrownApplication.class)
        .headless(false)
        .web(false) 
        //.bannerMode(Banner.Mode.OFF)
        .run(args);
	}
	
//	@Bean
//    public MainController frame() {
//        return new MainController();
//    }
}	