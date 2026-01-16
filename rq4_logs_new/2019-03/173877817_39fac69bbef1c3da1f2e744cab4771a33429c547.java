package seleniumPractise.technoCredits.webtableActionsIframeWindowHandle;

import java.awt.event.KeyEvent;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;

import util.Init;

public class ActionClassPract {

	
	public static void main(String[] args) throws InterruptedException {
		
		WebDriver driver = Init.initChromeDriver();
		//mouseHoverAction(driver);
		//dragAndDropScenario(driver);
		//rightClickDemo(driver);
		rightClickDemoAmazon(driver);
	}
	
	public static void dragAndDropScenario(WebDriver driver) throws InterruptedException {
		driver.get("http://jqueryui.com/droppable/");
		Thread.sleep(2000);
		driver.switchTo().frame(0);
		WebElement from = driver.findElement(By.cssSelector("div[id='draggable']"));
		WebElement to =driver.findElement(By.cssSelector("div[id=droppable]"));
		Actions action = new Actions(driver);
		action.dragAndDrop(from, to).perform();
		action.clickAndHold(from).moveToElement(to).release().build().perform();
		System.out.println("Element dropped on target");
		
	}
	
	public static void mouseHoverAction(WebDriver driver) throws InterruptedException {
		driver.get("https://opensource-demo.orangehrmlive.com/");
		driver.findElement(By.cssSelector("input[id=txtUsername]")).sendKeys("Admin");
		driver.findElement(By.cssSelector("input[id=txtPassword]")).sendKeys("admin123");
		driver.findElement(By.name("Submit")).click();
		Thread.sleep(1000);
		WebElement element = driver.findElement(By.id("menu_leave_viewLeaveModule"));
		WebElement element1 = driver.findElement(By.id("menu_leave_Entitlements"));
		Actions action = new Actions(driver);
		
		action.moveToElement(element).moveToElement(element1).build().perform();;	
		//System.out.println("on leave");
		
		//action.moveToElement(element1).build().perform();
		System.out.println("Done");
		
		driver.findElement(By.id("menu_leave_addLeaveEntitlement")).click();
        System.out.println("on expected page");
	}
	public static void rightClickDemo(WebDriver driver) throws InterruptedException {
		
		driver.get("https://www.google.co.in/");
		WebElement element = driver.findElement(By.linkText("Gmail"));
		Thread.sleep(2000);
		element.sendKeys(Keys.CONTROL + "t");
		((JavascriptExecutor) driver).executeScript("window.open('http://gmail.com/','_blank');");
		Actions action = new Actions(driver);
		//action.contextClick(element).perform();
		Thread.sleep(1000);
	//	action.keyDown(Keys.ARROW_DOWN).click().build().perform();
		
		System.out.println("done");
		
	}
	
	public static void rightClickDemoAmazon(WebDriver driver) throws InterruptedException {
		driver.get("https://www.amazon.com/");
		Thread.sleep(1000);
		Actions action = new Actions(driver);
		WebElement element = driver.findElement(By.linkText("Today's Deals"));
		action.contextClick(element).perform();
		Thread.sleep(1000);
		System.out.println("right click");
		//action.keyDown(Keys.ARROW_DOWN).build().perform();
	   //action.sendKeys(Keys.ARROW_DOWN).sendKeys(Keys.RETURN).build().perform();
		action.keyDown(Keys.ARROW_DOWN).click().keyUp(Keys.ARROW_DOWN).build().perform();
		System.out.println("open in new tab");
	}
	
	
}