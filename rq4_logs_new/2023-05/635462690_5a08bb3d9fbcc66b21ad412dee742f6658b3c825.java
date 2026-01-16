package Axel.AxelSpin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import Axel.AxelSpin.Controller.AxelSpinController;

@SpringBootApplication
public class AxelSpinApplication {

	public static void main(String[] args) {
		AxelSpinController server = new AxelSpinController();
		server.start();
	}

}