package dev.frilly.locket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * The main entrypoint of the program. Everything is bootstrapped from where.
 */
@SpringBootApplication
@ComponentScan(basePackages = "dev.frilly.locket")
public class LocketBackend {

  public static void main(final String[] args) {
    SpringApplication.run(LocketBackend.class, args);
  }

}