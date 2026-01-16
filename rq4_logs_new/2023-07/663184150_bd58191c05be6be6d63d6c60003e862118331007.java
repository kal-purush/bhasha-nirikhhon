package org.example;

import org.example.entity.Price;
import org.example.entity.Reading;
import org.example.entity.User;
import org.example.repository.PriceRepository;
import org.example.repository.ReadingRepository;
import org.example.repository.UserRepository;
import org.example.service.BillingFileWriterAndFolderCreator;
import org.example.service.BillingGenerator;
import org.example.service.BillingProcessor;
import org.example.util.PricesLoader;
import org.example.util.ReadingsLoader;
import org.example.util.UsersLoader;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication
@EnableJpaRepositories(basePackages = "org.example.repository")
public class Main {

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(UserRepository userRepository,
                                               ReadingRepository readingRepository,
                                               PriceRepository priceRepository,
                                               BillingGenerator billingGenerator,
                                               BillingFileWriterAndFolderCreator billingFileWriterAndFolderCreator) {


        return args -> {
            String inputDir = "C:\\Users\\ivani\\Desktop\\methodia\\MiniBilling\\src\\main\\resources";
            String outputDir = "C:\\Users\\ivani\\Desktop\\outputdir";

            userRepository.deleteAllInBatch();
            readingRepository.deleteAllInBatch();
            priceRepository.deleteAllInBatch();

            Map<String, User> users = UsersLoader.loadUsers(new File(inputDir, "users.csv"));
            users.values().stream()
                    .forEach(userRepository::save);
            System.out.println("Users have been saved to the database");


            List<Reading> readings = ReadingsLoader.loadReadings(new File(inputDir, "readings.csv"));
            readings.stream()
                    .forEach(readingRepository::save);
            System.out.println("Readings have been saved to the database");

            Map<String, List<Price>> prices = PricesLoader.loadPrices(inputDir);
            for (List<Price> priceList : prices.values()) {
                priceList.stream()
                        .forEach(priceRepository::save);
            }
            System.out.println("Prices have been saved to the database");

            BillingProcessor billingProcessor = new BillingProcessor(billingGenerator, readingRepository,userRepository, priceRepository, billingFileWriterAndFolderCreator);

            billingProcessor.processBillings(outputDir);
        };
    }
}






