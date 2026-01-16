package com.client.application;

import com.client.controller.ApplicationController;
import com.client.controller.ConnectionController;
import com.client.controller.ControllerPollution;
import com.client.view.ConnectionNamaiCity;



public class Main {
private static ConnectionNamaiCity test = new ConnectionNamaiCity();
	
	
	public static void main(String[] args) {
		ConnectionController controlleur = new ConnectionController(test);
		ApplicationController ac = new ApplicationController(test);
		ControllerPollution cp = new ControllerPollution(test);

	}

}