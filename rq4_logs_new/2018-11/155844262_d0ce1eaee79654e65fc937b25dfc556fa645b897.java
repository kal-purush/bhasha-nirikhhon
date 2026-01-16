package com.zealconnect.pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;
import org.openqa.selenium.support.PageFactory;

import com.zealconnect.testbase.BaseConfiguration;

public class LoginPage extends BaseConfiguration{
	
	@FindBy(how = How.XPATH, using = "//*[@id = 'LoginID']")
	WebElement userid;
	
	@FindBy(how = How.XPATH, using = "//*[@id = 'LoginPwd']")
	WebElement password;
	
	@FindBy(how = How.XPATH, using = "//*[@id = 'IDLoginUser']")
	WebElement loginButton;
	
	@FindBy(how = How.XPATH, using = "//*[@class = 'forgot-pwd']")
	WebElement passwordReset;
	
	@FindBy(how = How.XPATH, using = "//*[@class = 'register-btn']")
	WebElement registerNow;
	
	public LoginPage() {

		PageFactory.initElements(driver, this);
	}
	
	public void userLogin(String uname, String pword) {
		userid.sendKeys(uname);
		password.sendKeys(pword);
		loginButton.click();		
	}
}