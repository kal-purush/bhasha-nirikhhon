package org.github.barbero.spotippos;

import org.github.barbero.spotippos.services.DatamassService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Startup class.
 * <p>
 * It's implementing CommandLineRunner to make it's datamass on every startup,
 * once this application won't go to a cluster it's not a problem at all.
 *
 * @author Marcos Barbero
 */
@SpringBootApplication
public class Application implements CommandLineRunner {

    @Autowired
    private DatamassService datamassService;

    @Override
    public void run(String... args) throws Exception {
        this.datamassService.load();
    }

    public static void main(String... args) {
        SpringApplication.run(Application.class, args);
    }
}