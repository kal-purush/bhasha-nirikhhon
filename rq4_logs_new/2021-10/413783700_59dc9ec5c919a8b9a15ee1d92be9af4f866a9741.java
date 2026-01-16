package com.ss.ita.locators;

import org.openqa.selenium.By;


public enum SignInLoacators implements BaseLocator {
	SIGN_IN_BUTTON(By. cssSelector("form>button.primary-global-button")),
	EMAIL(By.cssSelector("input.successful-email-validation.ng-untouched.ng-pristine.ng-invalid")), ///EMAIL(By.name("email")),
	PASSWORD(By.id("password"));

	private final By path;
	
	SignInLoacators(By path) {
		this.path = path;
	}

	@Override
	public By getPath() {
		// TODO Auto-generated method stub
		return path;
	}

}