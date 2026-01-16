package backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Created by NXA-C.unltd on 2016/08/16.
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan
public class App
{
    public static void main(String [] args)
    {
        SpringApplication.run(App.class, args);
    }
}