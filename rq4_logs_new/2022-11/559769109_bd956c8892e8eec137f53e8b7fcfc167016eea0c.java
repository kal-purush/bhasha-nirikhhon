package stepDefination;

import static org.testng.Assert.assertEquals;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;

public class Calculator {

	WebDriver driver;
	@Given("calculator url is launched")
	public void calculator_url_is_launched() throws InterruptedException {
		System.setProperty("webdriver.chrome.driver",
				"C:\\Users\\003CO1744\\Downloads\\chromedriver_win32 (2)/chromedriver.exe");
		driver = new ChromeDriver();
		driver.get("https://www.calculator.net");
		Thread.sleep(5000);
		assertEquals("Calculator.net: Free Online Calculators - Math, Fitness, Finance, Science", driver.getTitle());
		String title = driver.getTitle();
		System.out.println("title1 is"+title);
	}

	@Then("Click on BMI Calculator")
	public void click_on_BMI_Calculator() {
	   driver.findElement(By.xpath("//a[contains(text(),'BMI Calculator')]")).click();
	   assertEquals("BMI Calculator", driver.getTitle());
	   String title2 = driver.getTitle();
		System.out.println("title2 is "+title2);
	}

	@And("Enter {string}, {string} and {string}")
	public void enter_and(String Age, String Height, String Weight) {
	   driver.findElement(By.id("cage")).clear();
	   driver.findElement(By.id("cage")).sendKeys(Age);
	   driver.findElement(By.id("cheightmeter")).clear();
	   driver.findElement(By.id("cheightmeter")).sendKeys(Height);
	   driver.findElement(By.id("ckg")).clear();
	   driver.findElement(By.id("ckg")).sendKeys(Weight);  
	}


	@Then("Calculate BMI")
	public void calculate_BMI() {
	    driver.findElement(By.xpath("//input[contains(@value,'Calculate')]")).click();
	}
	@And("capture the BMI Result")
	public void capture_the_BMI_Result() {
	   String result = driver.findElement(By.xpath("//h2[contains(text(),'Result')]/following-sibling::div/b")).getText();
	   System.out.println("Result is"+result);
	   driver.close();
	}


}