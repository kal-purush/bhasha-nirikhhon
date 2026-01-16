package hello;/*

  Created by smpark on 2016-01-19.

*/

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class Application {
    public static void main(String[] args){
        SpringApplication.run(Application.class, args);
        System.out.println("Spring Boot Started.");
    }
}
