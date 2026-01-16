package pageobjects;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class HomePage {
	WebDriver driver;

	public HomePage(WebDriver driver) {
		this.driver = driver;
		PageFactory.initElements(driver, this);
	}

	/**
	 * @doc Methods
	 * @name status
	 * @methodOf HomePage
	 * @description Gets Status Field on HomePage
	 */
	@FindBy(name = "xhpc_message")
	WebElement status;

	/**
	 * @doc Methods
	 * @name postButton
	 * @methodOf HomePage
	 * @description Gets postButton on HomePage
	 */
	@FindBy(xpath = "//button[contains(.,'Post')]")
	WebElement postButton;

	/**
	 * @doc Methods
	 * @name postStatus
	 * @methodOf HomePage
	 * @description post Status on HomePage
	 */
	public void postStatus(String textStatus) {
		status.click();
		status.sendKeys(textStatus);
		postButton.click();
	}
}