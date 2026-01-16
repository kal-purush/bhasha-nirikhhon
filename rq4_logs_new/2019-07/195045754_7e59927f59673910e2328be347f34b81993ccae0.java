package app;

import app.uap.Client;
import app.uap.messages.UssdBind;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(scanBasePackages = {"app.uap"})
@EnableConfigurationProperties
public class Application extends SpringBootServletInitializer {
    @Value("${name}")
    private String name;
    @Value("${age}")
    private Integer age;

    public static void main(String args[]) {
        SpringApplication.run(Application.class,args);
    }
    @Bean
    CommandLineRunner run() {
        return (String ...args) ->{
            Client client = new Client();
            client.setUp();
            //client.sendBytes(new UssdBind().encode());
        };
    }
}