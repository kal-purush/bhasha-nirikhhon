package com.micles92.importer;

import com.micles92.importer.logic.FileProcessorExecutor;
import com.micles92.importer.model.OperationMode;
import com.micles92.importer.service.UserAuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.util.*;

@SpringBootApplication
//@Profile("dev")
@EnableJpaRepositories(basePackages = {"com.micles92.importer.dao"})
@ComponentScan(basePackages = {"com.micles92.importer.service","com.micles92.importer.logic"})
@EntityScan(basePackages={"com.micles92.importer.model.entity"})
public class ImporterApplication implements CommandLineRunner{

    @Autowired
    private UserAuthenticationService userAuthenticationService;

    @Autowired
    private FileProcessorExecutor executor;

    @Override
    public void run(String... args) {

        // create a scanner so we can read the command-line input
        Scanner scanner = new Scanner(System.in);
        //  prompt for the user's name
        System.out.print("Podaj imie : ");
        String username = scanner.next();
        System.out.print("Podaj hasło: ");
        // get the age as an int
        String password = scanner.next();
        //TODO LOGOWANIE


        boolean isAuthenticated = userAuthenticationService.authenticate(username, password).isValid();

        if(isAuthenticated){

            System.out.print("Podaj tryb (1-ręczny, 2- autmatyczny): ");
            int mode  = scanner.nextInt();
            executor.execute(OperationMode.getOperationMode(mode), args);



        }else{
            System.out.println("Nie udało się zalogować");
        }


    }

    public static void main(String[] args) {
        SpringApplication.run(ImporterApplication.class, args);

    }
}