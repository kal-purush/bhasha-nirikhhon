package com.filmreview.controllers;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.filmreview.controller.AppController;

class AppControllerTest {

	@Test
	void test() {
		fail("Not yet implemented");
	}
	
	@Test
	void testUserRegistration() {
		//Instantiate the Controller
		AppController controller = new AppController();
		controller.registerUser("First", "Last", "testEmail@email.ie", "testingPassword", null);
		
	}

}