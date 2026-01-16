package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import baselibrary.BaseLibrary;

public class LoginTitle_Page extends BaseLibrary{
	
	public LoginTitle_Page() {
	PageFactory.initElements(driver, this);
	}

	@FindBy(xpath = "//a[text()='Login']")
	private WebElement login;
	@FindBy(xpath = "//input[@id=\"j_username\"]")
	private WebElement username;
	@FindBy(xpath = "//input[@id=\"j_password\"]")
	private WebElement password;
	@FindBy(xpath = "//input[@id=\"submitbutton\"]")
	private WebElement logenter;
	
	public void clickonlogin()
	{
		login.click();
		username.click();
		password.clear();
		logenter.clear();
	}

	
}