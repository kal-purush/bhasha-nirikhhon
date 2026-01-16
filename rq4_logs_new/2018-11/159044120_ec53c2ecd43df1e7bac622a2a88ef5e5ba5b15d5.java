package fi.haagahelia.BandProject;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import fi.haagahelia.BandProject.domain.Album;
import fi.haagahelia.BandProject.domain.AlbumRepository;

import fi.haagahelia.BandProject.domain.Band;
import fi.haagahelia.BandProject.domain.BandRepository;

@SpringBootApplication
public class BandProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(BandProjectApplication.class, args);
	}
	
	@Bean
	public CommandLineRunner demo(BandRepository brepository, AlbumRepository arepository) {
		return (args) -> {
			Band demo1 = new Band("Beatles", "Rock", "1960", 5);
			Band demo2 = new Band("Elvis Presley", "Rock", "1956", 4);
			Band demo3 = new Band("Justin Bieber", "Pop", "2009", 1);
			
			Album b1 = new Album("Beatles", "Please Please Me", "1963", 14, 5);
			Album b2 = new Album("Beatles", "With the Beatles", "1963", 14, 4);
			Album e1 = new Album("Elvis Presley", "Elvis", "1956", 12, 5);
			
			brepository.save(demo1);
			brepository.save(demo2);
			brepository.save(demo3);
			
			arepository.save(b1);
			arepository.save(b2);
			arepository.save(e1);
		};
	}
}