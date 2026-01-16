package org.test;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.Select;

import io.github.bonigarcia.wdm.WebDriverManager;

public class BaseClass {
	static WebDriver driver;
	public static WebDriver launchbrowser(String browserName) {
		
//***********for single browser launch************		
//		WebDriverManager.chromedriver().setup();
//		driver=new ChromeDriver();
//		return driver;
		
//**************using if else condition *************
//	if(browserName.equals("chrome")) {
//		WebDriverManager.chromedriver().setup();
//		driver=new ChromeDriver();
//	}
//	else if(browserName.equals("edge")) {
//		WebDriverManager.edgedriver().setup();
//		driver=new EdgeDriver();	
//	}
//	else if(browserName.equals("firefox")) {
//		WebDriverManager.firefoxdriver().setup();
//		driver=new FirefoxDriver();	
//	}
//	else {
//		System.out.println("Invalid");
//	}
//	return driver;
//*************other way in switch case**********
	switch(browserName) {
	case "chrome":
	WebDriverManager.chromedriver().setup();
	driver=new ChromeDriver();
	break;
	case "edge":
		WebDriverManager.edgedriver().setup();
		driver=new EdgeDriver();
		break;
	case "firefox":
		WebDriverManager.firefoxdriver().setup();
		driver=new FirefoxDriver();	
		break;
	}
	return driver;
	
	}

	public static void launchUrl(String url) {
		driver.get(url);
		driver.manage().window().maximize();
	}
		
	
	public static void implicitWait(long time) {
		
	driver.manage().timeouts().implicitlyWait(time,TimeUnit.SECONDS);
		
	}	
	

//senkeys
	public static void sendKeys(WebElement e,String value) {
		e.sendKeys(value);		
	}
//	click
	public static void click(WebElement e) {
		e.click();
		
	}
//current url

	public static String getcurrentUrl() {
		String currentUrl=driver.getCurrentUrl();
		return currentUrl;
}
//title
	public static  void gettitle() {
		String title=driver.getTitle();
}
//quit
	public static void quit() {
		driver.quit();
	}

//gettext
	public static String getText(WebElement e) {
		String text = e.getText();
		return text;
	}
//getattribute
	public static String getAttribute(WebElement e) {
		String attribute = e.getAttribute("value");
		return attribute;
	}
// actions
//	movetoelement
	public static void movetoElement(WebElement e) {
		Actions a=new Actions(driver);
		a.moveToElement(e).perform();	
	}
//	action--->click
	public static void click() {
		Actions a=new Actions(driver);
		a.click();
	}
//	action-->doubleclick
	public static void doubleclick() {
		Actions a=new Actions(driver);
		a.doubleClick();
	}
//	actions-->rightclick
	public static void rightclick() {
		Actions a=new Actions(driver);
		a.contextClick();
	}
//action-->drag and drop
	public static  void dragAndDrop(WebElement src,WebElement des) {
		Actions a=new Actions(driver);
		a.dragAndDrop(src, des).perform();
	}
//select
//	selectbyindex
	
  public static void selectByIndex(WebElement e,int index) {
	Select s=new Select(e);
	s.selectByIndex(index);
	}
	
//	selectbyvalue
  
  public static void selectByValue(WebElement e,String value) {
		Select s=new Select(e);
		s.selectByValue(value);
	}	
//selectby visbletext
  public static void selectByVisbelText(WebElement e,String value) {
	Select s=new Select(e);
	s.selectByVisibleText(value);
}
// single or multiple
  public static void isMultiple(WebElement e) {
	  Select s=new Select(e);
	 s.isMultiple();
}
	
//deselectbyindex
  public static void deselectByIndex(WebElement e,int index) {
	Select s=new Select(e);
	s.deselectByIndex(index);
}
//	deselectbyalue
  public static void deselectByValue(WebElement e,String value) {
	Select s=new Select(e);
	s.deselectByValue(value);
}
//	deslectbytext
  public static  void deSelectByVisibleText(WebElement e,String value) {
	  Select s=new Select(e);
	s.deselectByVisibleText(value);
}
//	deselect all
  public static void deSelectAll(WebElement e) {
	Select s=new Select(e);
	s.deselectAll();
}

//get all selected option
  public static void getAllSelectedOptions(WebElement e) {
	Select s=new Select(e);
	s.getAllSelectedOptions();
}
//	get first selected options
  public static void firstSelectedOptions(WebElement e) {
	Select s=new Select(e);
  s.getFirstSelectedOption();
}
//	get all options
  public static void getOptions(WebElement e) {
    Select s=new Select(e);
    s.getOptions();
}

	




}


