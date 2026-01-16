package com.smart_cities.citizen.data;

import com.smart_cities.citizen.model.Citizen;
import com.smart_cities.citizen.model.Meter;
import com.smart_cities.citizen.repository.CitizenRepository;
import com.smart_cities.citizen.repository.MeterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class LoadData {
    private static final Logger log = LoggerFactory.getLogger(LoadData.class);

    @Bean
    CommandLineRunner initDatabase(CitizenRepository citizenRepository, MeterRepository meterRepository) {
        return args -> {
            if (citizenRepository.count() > 0) {
                return;
            }
            for (long i = 0; i < 100; i++) {
                log.info("Preloading " + citizenRepository.save(new Citizen(1L)));
                log.info("Preloading " + citizenRepository.save(new Citizen(2L)));
                log.info("Preloading " + citizenRepository.save(new Citizen(3L)));
            }

            if (meterRepository.count() > 0) {
                return;
            }
            for (long i = 0; i < 50; i++) {
                log.info("Preloading " + meterRepository.save(new Meter(1L)));
                log.info("Preloading " + meterRepository.save(new Meter(2L)));
                log.info("Preloading " + meterRepository.save(new Meter(3L)));
            }
        };
    }
}