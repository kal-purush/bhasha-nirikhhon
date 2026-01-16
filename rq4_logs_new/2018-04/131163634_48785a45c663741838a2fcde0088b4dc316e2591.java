package com.example.Template;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.util.Arrays;
import java.util.Collection;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {

		SpringApplication.run(DemoApplication.class, args);
	}
}

@RestController
class ReservationRestController {

	private final ReservationRepository reservationRepository;

	@Autowired
	public ReservationRestController(ReservationRepository reservationRepository) {
		this.reservationRepository = reservationRepository;
	}

	@RequestMapping (method = RequestMethod.GET, value = "/reservations")
	Collection<Reservation> reservations(){
		return this.reservationRepository.findAll();
	}

}

@Component
class RepositoryCommandLineRunner implements CommandLineRunner {

	private final ReservationRepository reservationRepository;


	@Override
	public void run(String... args) throws Exception {

		Arrays.asList("Андрюха,Леха,Михаил,Антон".split(","))
				.forEach(n->this.reservationRepository.save(new Reservation(n)));

		this.reservationRepository.findAll().forEach(r-> System.out.println(r));
	}

	@Autowired
	public RepositoryCommandLineRunner(ReservationRepository reservationRepository) {
		this.reservationRepository = reservationRepository;
	}
}

interface ReservationRepository extends JpaRepository<Reservation, Long> {
	Collection<Reservation> findByReservationName(String rn);

}