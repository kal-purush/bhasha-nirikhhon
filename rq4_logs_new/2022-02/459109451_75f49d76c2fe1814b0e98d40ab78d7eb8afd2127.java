package com.cs.logfile.time.identifier.logfileidentifier;

import com.cs.logfile.time.identifier.logfileidentifier.service.LogFileIdentifireService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;

@SpringBootApplication
public class LogfileIdentifierApplication implements CommandLineRunner{
	Logger logger= LogManager.getLogger(LogfileIdentifierApplication.class);

	@Autowired
	LogFileIdentifireService logFileIdentifireService;

	public static void main(String... args) {
		SpringApplication app = new SpringApplication(LogfileIdentifierApplication.class);
		app.run(args);
	}

	@Override
	public void run(String... args) {
		logger.info("calling process File service method");
		logFileIdentifireService.processFile(args[0]);
		//logFileIdentifireService.get();
	}

}