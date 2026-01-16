package com.sgtesting.actitime.stepdefinitions;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class ActiTimePages {
	
	public ActiTimePages(WebDriver oBrowser)
	{
		PageFactory.initElements(oBrowser, this);
	}
	
	private WebElement username;
	public WebElement getUserName()
	{
		return username;
	}
	
	private WebElement pwd;
	public WebElement getPassword()
	{
		return pwd;
	}
	
	@FindBy(xpath="//*[@id='loginButton']/div")
	private WebElement oLogin;
	public WebElement getLoginBtn()
	{
		return oLogin;
	}
	
	@FindBy(linkText="Logout")
	private WebElement oLogout;
	public WebElement getLogoutLink()
	{
		return oLogout;
	}

}