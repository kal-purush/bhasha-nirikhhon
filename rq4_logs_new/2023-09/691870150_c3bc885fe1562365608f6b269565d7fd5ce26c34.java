package com.hex.smart.web.pageobjectlibrary;

import java.time.Duration;

import com.hex.smart.web.pagelocators.WebLoginPageLocators;

import net.serenitybdd.core.pages.PageObject;
import net.thucydides.core.annotations.Step;

public class WebLoginPageObject extends PageObject{

	@Step("Open Browser")
	public void openAndNavigate() {
		open();
	}
	
	@Step("Enter Username")
	public void enterUsername(String username) {
		getDriver().manage().timeouts().implicitlyWait(Duration.ofSeconds(5));
		$(WebLoginPageLocators.WEB_LOGINPAGE_USERNAME).sendKeys(username);
	}
	
	@Step("Enter Password")
	public void enterPassword(String password) {
		getDriver().manage().timeouts().implicitlyWait(Duration.ofSeconds(5));
		$(WebLoginPageLocators.WEB_LOGINPAGE_PASSWORD).sendKeys(password);
	}
	
	@Step("Click Login")
	public void clickLogin() {
		$(WebLoginPageLocators.WEB_LOGINPAGE_LOGIN_BTN).click();
	}
	
	@Step("Close Browser")
	public void closeBrowser() {
		getDriver().close();
	}
	
}