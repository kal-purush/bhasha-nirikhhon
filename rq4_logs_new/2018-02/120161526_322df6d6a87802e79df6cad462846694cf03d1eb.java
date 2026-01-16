package com.category.mttype;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class Category1 {

	public static void main(String[] args) throws Exception {

		Category1 Test1 = new Category1();
		Test1.TestMT299();
		//Test1.TestMT200();

	}

	public void TestMT299() throws Exception {

		WebDriver driver = new ChromeDriver();
		driver.manage().window().maximize();
		driver.get("http://10.33.44.30:8080/nSMARTLite/");

		driver.findElement(By.name("userName")).sendKeys("SUPER1");
		driver.findElement(By.name("password")).sendKeys("SUPER1@123");

		driver.findElement(By
				.xpath(".//div[contains(@class, 'x-btn x-box-item x-toolbar-item x-btn-default-small x-icon-text-left x-btn-icon-text-left x-btn-default-small-icon-text-left')]"))
				.click();
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
		driver.findElement(By.id("Message Management-shortcut")).click();

		WebDriverWait wait = new WebDriverWait(driver, 100);

		wait.until(ExpectedConditions.visibilityOfElementLocated(
				By.xpath(".//div[contains(@class, 'x-grid-group-title') and text()='Category 2 (11)']")));
		WebElement category2 = driver
				.findElement(By.xpath(".//div[contains(@class, 'x-grid-group-title') and text()='Category 2 (11)']"));
		category2.click();
		category2.click();
		WebElement category2Body = driver
				.findElement(By.xpath(".//tr[contains(@class, 'x-grid-group-body') and contains(@id, 'Category 2')]"));
		List<WebElement> categoryList = category2Body.findElements(By.tagName("tr"));
		WebElement mt299 = categoryList.get(11);
		Actions action = new Actions(driver);

		action.moveToElement(mt299).doubleClick().perform();
		driver.findElement(By.xpath(".//input[contains(@name, 'receiverBicCode') ]")).sendKeys("TTBLBDDHXXX");
		driver.findElement(By.xpath(".//input[contains(@name, 'textfield-2294-inputEl') ]"))
				.sendKeys("isma.ety@naztech.us.com");

		WebElement field20 = driver
				.findElement(By.xpath(".//fieldset[.//div[text()='F20 (Transaction Reference Number)']]"));
		field20.findElement(By.xpath(".//input[contains(@name, 'textfield-2262-inputEl') ]"))
				.sendKeys("897215321987289");
		wait.until(ExpectedConditions
				.visibilityOfElementLocated(By.xpath(".//fieldset[.//div[text()='F21 (Related Reference)']]")));
		List<WebElement> field21List = driver
				.findElements(By.xpath(".//fieldset[.//div[text()='F21 (Related Reference)']]"));
		WebElement field21Legend = field21List.get(1).findElement(By.tagName("legend"));
		field21Legend.click();
		field21List.get(1).findElement(By.xpath(".//input[contains(@name, 'textfield-2265-inputEl') ]"))
				.sendKeys("8972153");

		WebElement field79 = driver.findElement(By.xpath(".//fieldset[.//div[text()='F79 (Narrative)']]"));
		field79.findElement(By.xpath(".//textarea[contains(@name, '79_0') ]")).sendKeys("kasdglkjasdlsdryhsdg");
		WebElement saveBtn = driver.findElement(By.xpath(".//button[.//span[text()='Route']]"));
		saveBtn.click();
		Thread.sleep(500);
		driver.findElement(By.id("button-1005-btnInnerEl")).click();
	}
	
	/*public void TestMT200() throws Exception {

		WebDriver driver = new ChromeDriver();
		driver.manage().window().maximize();
		driver.get("http://10.33.44.30:8080/nSMARTLite/");

		driver.findElement(By.name("userName")).sendKeys("SUPER1");
		driver.findElement(By.name("password")).sendKeys("SUPER1@123");

		driver.findElement(By
				.xpath(".//div[contains(@class, 'x-btn x-box-item x-toolbar-item x-btn-default-small x-icon-text-left x-btn-icon-text-left x-btn-default-small-icon-text-left')]"))
				.click();
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
		driver.findElement(By.id("Message Management-shortcut")).click();
		WebDriverWait wait = new WebDriverWait(driver, 100);

		wait.until(ExpectedConditions.visibilityOfElementLocated(
				By.xpath(".//div[contains(@class, 'x-grid-group-title') and text()='Category 2 (11)']")));
		WebElement category2 = driver
				.findElement(By.xpath(".//div[contains(@class, 'x-grid-group-title') and text()='Category 2 (11)']"));
		category2.click();
		category2.click();
		WebElement category2Body = driver
				.findElement(By.xpath(".//tr[contains(@class, 'x-grid-group-body') and contains(@id, 'Category 2')]"));
		List<WebElement> categoryList = category2Body.findElements(By.tagName("tr"));
		WebElement mt200 = categoryList.get(1);
		Actions action = new Actions(driver);

		action.moveToElement(mt200).doubleClick().perform();
		driver.findElement(By.xpath(".//input[contains(@name, 'receiverBicCode') ]")).sendKeys("TTBLBDDHXXX");
		driver.findElement(By.xpath(".//input[contains(@name, 'textfield-2324-inputEl') ]"))
				.sendKeys("isma.ety@naztech.us.com");

		WebElement field20 = driver
				.findElement(By.xpath(".//fieldset[.//div[text()='F20 (Transaction Reference Number)']]"));
		field20.findElement(By.xpath(".//input[contains(@name, 'textfield-2262-inputEl') ]"))
				.sendKeys("897215321987289");
		WebElement field32A = driver.findElement(By.xpath(".//fieldset[.//div[text()='F32A (Value Date/Currency Code/Amount)']]"));
		driver.findElement(By.xpath(".//input[contains(@name, 'date')]")).sendKeys("Sep 19, 2017");
		driver.findElement(By.xpath(".//input[contains(@name, 'currency')]")).sendKeys("USD");
		driver.findElement(By.xpath(".//input[contains(@name, 'amount')]")).sendKeys("100");

 }*/
}