package com.amaka.springexcel;

import com.amaka.springexcel.service.ApplicationInitializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringExcelApplication implements CommandLineRunner {

@Autowired
    private ApplicationInitializer applicationInitializer;

    public static void main(String[] args) {
        SpringApplication.run(SpringExcelApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        applicationInitializer.init();
    }
}
