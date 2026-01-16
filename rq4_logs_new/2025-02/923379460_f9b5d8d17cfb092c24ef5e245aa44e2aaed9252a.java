package pages;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import utilities.PageUtility;

public class HomePage {
	public WebDriver driver;

	public HomePage(WebDriver driver) {
		this.driver = driver;
		PageFactory.initElements(driver, this);
	}

	@FindBy(linkText = "Admin")
	private WebElement clickAdminButton;
	@FindBy(linkText = "Logout")
	private WebElement clickOnLogout;
	@FindBy(xpath = "//button[@class='btn btn-dark btn-block']")
	private WebElement signinButton;

	public HomePage clickOnAdminButton() {
		PageUtility.clickOnElement(clickAdminButton);
		return new HomePage(driver);
	}

	public LoginPage clickOnLogoutButton() {
		PageUtility.clickOnElement(clickOnLogout);
		return new LoginPage(driver);
	}

	public boolean isSigninButtonDisplayed() {
		return signinButton.isDisplayed();
	}

}