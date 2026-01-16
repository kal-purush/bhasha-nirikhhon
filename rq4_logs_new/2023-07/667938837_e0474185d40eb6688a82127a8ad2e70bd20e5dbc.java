package com.got.familytree;

//import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.got.familytree.service.CharacterService;



@SpringBootApplication
public class GotFamilyTreeApplication {
	
//	@Autowired
//	private CharacterService characterService;
//
//    public GotFamilyTreeApplication(CharacterService characterService) {
//        this.characterService = characterService;
//    }

	public static void main(String[] args) {
		SpringApplication.run(GotFamilyTreeApplication.class, args);
	}
	
	@Bean
	CommandLineRunner runner(CharacterService characterService) {
		return args->{
			characterService.storeCharactersFromJsonFile();
		};
		
	}

}