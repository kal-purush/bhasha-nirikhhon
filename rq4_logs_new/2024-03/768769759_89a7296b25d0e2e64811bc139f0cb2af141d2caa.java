package bg.fmi.web.lab.racereventmanagment;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import bg.fmi.web.lab.racereventmanagment.service.RacerService;

@SpringBootApplication
public class RacerEventManagmentApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(RacerEventManagmentApplication.class, args);
    }
    @Autowired
    private RacerService racerService;


    @Override
    public void run(String... args) throws Exception {
        racerService.createRacer("Test", "Test", 18);
        racerService.createRacer("Test1", "Test1", 18);
        racerService.createRacer("Test2", "Test2", 18);

        racerService.getAllRacers().stream().forEach(el -> System.out.println(el.getFirstName()));
    }
}