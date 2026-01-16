package com.herokuapp.theinternet;

import java.time.Duration;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class ExceptionsTests {
	private WebDriver driver;
	String testurl = "https://practicetestautomation.com/practice-test-exceptions/";
	
	@Parameters({"browser"})
	@BeforeMethod(alwaysRun = true)
	private void setUp(@Optional("chrome") String browser) {
		//Create driver
		switch(browser) {
		case "chrome":
			System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
			driver = new ChromeDriver();		
			break;
		case "firefox":
			System.setProperty("webdriver.gecko.driver", "src/main/resources/geckodriver.exe");
			driver = new FirefoxDriver();				
			break;
		default:
			System.out.println("Starting with default Chrome, error starting browser " + browser);
			System.setProperty("webdriver.chrome.driver", "src/main/resources/chromedriver.exe");
			driver = new ChromeDriver();		
			break;
			}
		// sleep for 2 seconds
		driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(2));
	}
	
	@Test(priority = 1, groups = { "negativeTests" })
	public void noSuchExceptionTest() {
		System.out.println("Starting NoSuchException test.");
		//Maximize browser window
		driver.manage().window().maximize();
		
		//Go to the page
		driver.get(testurl);
		System.out.println("URL " + testurl + " has been opened.");	
		
		//Find the Add button
		WebElement addButton =  driver.findElement(By.id("add_btn"));
		//Click on Add button
		addButton.click();
		
		//Implicit wait test... not ideal.
		//driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(7));
		
		//Explicit wait test
		WebDriverWait lWait = new WebDriverWait(driver, Duration.ofSeconds(10));
		//Look for the 2nd row by xpath 
		WebElement secondRow = lWait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='row2']/input")));
		Assert.assertTrue(secondRow.isDisplayed(), "Second row is not visible.");
	}

	@Test(priority = 1, groups = { "negativeTests" })
	public void elementNotInteractableExceptionTest() {
		System.out.println("Starting ElementNotInteractableException test.");
		//Maximize browser window
		driver.manage().window().maximize();
		
		//Go to the page
		driver.get(testurl);
		System.out.println("URL " + testurl + " has been opened.");	
		
		//Find the Add button
		WebElement addButton =  driver.findElement(By.id("add_btn"));
		//Click on Add button
		addButton.click();
		
		//Explicit wait test
		WebDriverWait lWait = new WebDriverWait(driver, Duration.ofSeconds(10));
		//Look for the 2nd row by xpath 
		WebElement secondRow = lWait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='row2']/input")));
		secondRow.sendKeys("Mooshu is a cat.");
		WebElement saveButton = driver.findElement(By.xpath("//div[@id='row2']/button[@name='Save']"));
		saveButton.click();
		
		String expectedMsg = "Row 2 was saved";
		String actualMsg = driver.findElement(By.id("confirmation")).getText();
		Assert.assertEquals(actualMsg, expectedMsg, "Confirmation message is not the expected one.");

	}
	
	@Test (priority = 1, groups = { "negativeTests" })
	private void invalidElementStateExceptionTest() {
//		Open page
		System.out.println("Starting invalidElementStateException test.");
		//Maximize browser window
		driver.manage().window().maximize();
		
		//Go to the page
		driver.get(testurl);
		System.out.println("URL " + testurl + " has been opened.");	

		WebDriverWait lWait = new WebDriverWait(driver, Duration.ofSeconds(10));
		//Look for the initial row. 
		WebElement firstInputFld = lWait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[@id='row1']/input")));
		//WebElement firstInputFld = driver.findElement(By.className("input-field"));

//		Clear input field
		WebElement editButton = driver.findElement(By.name("Edit"));
		editButton.click();
		lWait.until(ExpectedConditions.elementToBeClickable(firstInputFld));
		firstInputFld.clear();
		
		String testString = "Mooshu";
//		Type text into the input field
		firstInputFld.sendKeys(testString);
		WebElement saveButton = driver.findElement(By.name("Save"));
		saveButton.click();
//		Verify text changed
		String actualString = firstInputFld.getAttribute("value");
		Assert.assertEquals(actualString, testString, "Input text is not the expected one.");
		
		String expectedMsg = "Row 1 was saved";
		//String actualMsg = driver.findElement(By.id("confirmation")).getText();
		String actualMsg = lWait.until(ExpectedConditions.visibilityOfElementLocated(By.id("confirmation"))).getText();
		Assert.assertEquals(actualMsg, expectedMsg, "Confirmation message is not the expected one.");		
	}

	@Test (priority = 1, groups = { "negativeTests" })
	private void staleElementReferenceExceptionTest() {
//		Open page
		System.out.println("Starting staleElementReferenceException test.");
		// Maximize browser window
		driver.manage().window().maximize();

		// Go to the page
		// set URL
		driver.get(testurl);
		System.out.println("URL " + testurl + " has been opened.");

		WebDriverWait lWait = new WebDriverWait(driver, Duration.ofSeconds(10));
		// Wait until test element is visible.
		lWait.until(ExpectedConditions.visibilityOfElementLocated(By.id("instructions")));

//		Click Add button
		WebElement addButton =  driver.findElement(By.id("add_btn"));
		addButton.click();
		//After click, the instruction should be not visible.
		Assert.assertTrue(lWait.until(ExpectedConditions.invisibilityOfElementLocated(By.id("instructions"))), 
				"Instruction is still displayed");
	}

	
	@Test (priority = 1, groups = { "negativeTests" })
	private void timeoutExceptionTest() {
//		Open page
		System.out.println("Starting timeoutException test.");
		// Maximize browser window
		driver.manage().window().maximize();

		// Go to the page
		// set URL
		driver.get(testurl);
		System.out.println("URL " + testurl + " has been opened.");


		WebDriverWait lWait = new WebDriverWait(driver, Duration.ofSeconds(6));
		//	Click Add button
		lWait.until(ExpectedConditions.visibilityOfElementLocated(By.id("add_btn")));
		WebElement addButton =  driver.findElement(By.id("add_btn"));
		addButton.click();

//		Wait for 3 seconds for the second input field to be displayed
//		Verify second input field is displayed
		//Assert.assertTrue(lWait.until(ExpectedConditions.invisibilityOfElementLocated(By.xpath("//div[@id='row2']/input")));
		Assert.assertTrue(lWait.until(ExpectedConditions.attributeToBe(By.xpath("//div[@id='row2']/input"), "New row is not visible.", "")));
	}	
	@AfterMethod(alwaysRun = true)
	private void tearDown() {
		// Close browser
		driver.quit();
	}	
}