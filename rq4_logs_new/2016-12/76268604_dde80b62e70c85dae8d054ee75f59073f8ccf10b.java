package com.shana.biochemistry;

import org.springframework.boot.ApplicationPid;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.autoconfigure.mail.MailSenderAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.io.File;
import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@SpringBootApplication
@EnableJpaRepositories(basePackages="com.shana.biochemistry")
@ComponentScan({"com.shana.biochemistry"})
public class Application {

    /*@Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSource primaryDataSource() {
        return DataSourceBuilder.create().build();
    }*/

    /*@PostConstruct
    private void handlePid() throws IOException {
        File file = new File("application.pid");
        new ApplicationPid().write(file);
        file.deleteOnExit();
    }*/

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}