package com.seachangesimulations.scorp;

import java.util.LinkedHashMap;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.seachangesimulations.scorp.domain.Phase;
import com.seachangesimulations.scorp.service.ObjectService;

@SpringBootApplication
public class ScorpApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScorpApplication.class, args);
	}
	
}