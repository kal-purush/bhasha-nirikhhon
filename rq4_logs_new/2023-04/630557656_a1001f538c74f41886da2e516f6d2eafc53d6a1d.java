package com.aplicatie_donare_de_sange.centru_de_donare;

import com.aplicatie_donare_de_sange.centru_de_donare.Repository.UserRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
public class CentruDeDonareApplication {

    public static void main(String[] args) {
        SpringApplication.run(CentruDeDonareApplication.class, args);
    }

}