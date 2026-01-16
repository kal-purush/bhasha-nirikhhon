package com.bae.crohnsFoodDiary.selenium;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT) // Loads the context
@ActiveProfiles("test")
public class SeleniumFoodTest {

	@LocalServerPort
	private int port;

	private RemoteWebDriver driver;

	@BeforeEach
	void setup() {
		ChromeOptions options = new ChromeOptions();
		options.setHeadless(false);
		this.driver = new ChromeDriver(options);
		this.driver.manage().window().maximize();

	}

	@Test
	void test() {
		this.driver.get("http://localhost:" + port);
	}

	@Test
	void createFoodTest() {
		this.driver.get("http://localhost:" + port);

		// get fields and submit button of record food form
		WebElement foodNameInput = this.driver.findElement(By.xpath("//*[@id=\"foodName\"]"));
		WebElement foodTypeInput = this.driver.findElement(By.xpath("//*[@id=\"foodType\"]"));
		WebElement foodCaloriesInput = this.driver.findElement(By.xpath("//*[@id=\"foodCalories\"]"));
		WebElement foodEffectInput = this.driver.findElement(By.xpath("//*[@id=\"foodEffect\"]"));
		WebElement submitButton = this.driver.findElement(By.id("foodSubmit"));

		// send data to the fields and click submit
		foodNameInput.sendKeys("Garlic");
		foodTypeInput.sendKeys("Vegetable");
		foodCaloriesInput.sendKeys("5");
		foodEffectInput.sendKeys("Positive");
		submitButton.click();

		// wait for the response
		this.driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);

		// get response
		WebElement response = this.driver.findElement(By.xpath("//*[@id=\"foodOutput\"]/tr[2]/td[1]"));

		// check the response matches
		Assertions.assertTrue(response.getText().contains("Garlic"));

	}

	@Test
	void deleteFoodTest() {
		this.driver.get("http://localhost:" + port);

		// add a food record to the website
		// get fields and submit button of record food form
		WebElement foodNameInput = this.driver.findElement(By.id("foodName"));
		WebElement foodTypeInput = this.driver.findElement(By.xpath("//*[@id=\"foodType\"]"));
		WebElement foodCaloriesInput = this.driver.findElement(By.xpath("//*[@id=\"foodCalories\"]"));
		WebElement foodEffectInput = this.driver.findElement(By.xpath("//*[@id=\"foodEffect\"]"));
		WebElement submitButton = this.driver.findElement(By.id("foodSubmit"));
		// send data to the fields and click submit
		foodNameInput.sendKeys("Garlic");
		foodTypeInput.sendKeys("Vegetable");
		foodCaloriesInput.sendKeys("5");
		foodEffectInput.sendKeys("Positive");
		submitButton.click();

		// wait for the response
		this.driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);

		// get delete button
		WebElement deleteButton = this.driver.findElement(By.xpath("//*[@id=\"foodButtons\"]/button"));

		// click the delete
		deleteButton.click();
		// wait for the response
		this.driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);

		WebElement response = this.driver.findElement(By.xpath("//*[@id=\"foodOutput\"]"));
		Assertions.assertTrue(response.getText().isEmpty());

	}

	@AfterEach
	void tearDown() {
		this.driver.close();
	}

}