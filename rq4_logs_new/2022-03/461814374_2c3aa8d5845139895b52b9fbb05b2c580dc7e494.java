package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.util.List;

public class BaggageAndExtras extends BasePage{

	WebDriver driver;

	public BaggageAndExtras(WebDriver driver) {
		super(driver);
		this.driver = driver;
		PageFactory.initElements(driver, this);
	}

	@FindBy(css = "[data-testid='checkout_extras_inner_next']")
	WebElement nextBtn;

	@FindBy(xpath = "//div[contains(text(), 'Luggage')]")
	List<WebElement> luggagePage;

	public boolean checkIfPageExists () {
		return isElementsPresent(luggagePage);
	}

	public void chooseMeal(String passengersNum) {
		for (int i = 0; i < Integer.parseInt(passengersNum); i++) {
			randomIndexFromDropDown(driver.findElements(By.cssSelector(".InputSelect-module__field___10SP5")).get(i));
		}
	}

	public void clickNext() throws InterruptedException {
		clickElement(nextBtn);
	}
}