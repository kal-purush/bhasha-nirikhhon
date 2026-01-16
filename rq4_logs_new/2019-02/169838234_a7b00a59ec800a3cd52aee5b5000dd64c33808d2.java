package com.game.plane;

import com.game.plane.config.WebConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ComponentScan({
        "com.game.plane.controller",
        "com.game.plane.service"
})
@Import({
        WebConfig.class
})
public class PlaneApplication {

    public static void main(String[] args) {
        SpringApplication.run(PlaneApplication.class, args);
    }

}
