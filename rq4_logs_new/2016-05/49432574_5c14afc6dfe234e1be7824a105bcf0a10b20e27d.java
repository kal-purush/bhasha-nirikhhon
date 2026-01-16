package com.totalwine.test.checkout;

/*
 *  New User Registration using random credentials or credential from DB 
 *  Work flow : 
 *  1. Click on Sign In / Register link on the header
 *  2. Click on Register button
 *  3. Fill up all the required information in  "Create your account" page
 *  5. Click on the Register button
 *  6. Verify welcome message displays after account creation. 

 * Technical Modules:
 * 	1. DataProvider: Checkout test input parameters
 * 	2. BeforeMethod (Test Pre-requisites):
 * 			Invoke WebDriver
 * 			Maximize browser window
 * 	3. Test (Workflow)
 * 	4. AfterMethod
 * 			Take screenshot, in case of failure
 * 			Close WebDriver
 * 	5. AfterClass
 * 			Quit WebDriver
 */

import java.io.IOException;
import java.util.Random;
import jxl.read.biff.BiffException;
import org.testng.annotations.Test;
import org.testng.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import com.totalwine.test.config.ConfigurationFunctions;
import com.totalwine.test.pages.PageGlobal;
import com.totalwine.test.trials.Browser;
import com.totalwine.test.actions.*;

public class NewUserRegistration extends Browser {

	@DataProvider(name="CheckoutParameters")
    public Object[][] createData() {
    	Object[][] retObjArr=ConfigurationFunctions.getTableArray(ConfigurationFunctions.resourcePath,"Checkout", "Registration");
        return(retObjArr);
    } 
	
	@BeforeMethod
	  public void setUp() throws Exception {
	    driver.manage().window().maximize();	
		 }  
	
	@Test (dataProvider = "CheckoutParameters")
	public void WebAccountRegistrationTest (String Location,String StoreName,String PDP,String Quantity,String ShipOption,String FirstName,
			String LastName,String Company,String Address1,String Address2,String City,String State,String Zip,String Email,String Password,
			String Phone,String CreditCard,String ExpirationMonth,String ExpirationYear,String CVV)
					
					throws InterruptedException, BiffException, IOException {
		
		logger=report.startTest("New User Registration using random credentials or credential from DB");
		
		//** Random E-mail Creation
		Random rand = new Random();
	    int randomNum = rand.nextInt((1000 - 1) + 1) + 1;
	    int randomNum_2 = rand.nextInt((1000 - 1) + 1) + 1;

		driver.get(ConfigurationFunctions.locationSet+Location);
		PageLoad(driver); // Will not trigger the next control until loading the page
		
		//** By Passing Age Gate and Welcome Modal
		Checkout.AgeGateWelcome(driver);
		PageLoad(driver); // Will not trigger the next control until loading the page
		
	    // ** Checking for survey pop-up
	    Checkout.SurverPopup(driver);
	    driver.findElement(PageGlobal.TopNavAccount).click();
	    Thread.sleep(2000);
	    driver.findElement(PageGlobal.SignUp).click();
	    Thread.sleep(2000);
	
	 // ** Filling up "Create your account" page
	    driver.findElement(By.cssSelector("#firstName")).sendKeys(FirstName);
	    driver.findElement(By.cssSelector("#lastName")).sendKeys(LastName);
	    Thread.sleep(2000);

	    // *** Register Email from DB
//	    driver.findElement(By.cssSelector("#email")).sendKeys(Email);
//	    driver.findElement(By.cssSelector("#checkEmail")).sendKeys(Email);
	    
	    // *** Register Random Email
	    driver.findElement(By.cssSelector("#email")).sendKeys("autoemail_"+randomNum+"."+randomNum_2+"@totalwine.com");
    	String email = driver.findElement(By.cssSelector("#email")).getAttribute("value");
    	System.out.println("Registered Email Address: "+email);
	    driver.findElement(By.cssSelector("#pwd")).sendKeys(Password);
	    driver.findElement(By.cssSelector("#phone")).sendKeys(Phone);
	    Thread.sleep(2000);
	    WebElement scroll2 = driver.findElement(By.cssSelector("#btnnuregisteration"));  
	    scroll2.sendKeys(Keys.PAGE_DOWN); //  ** Scrolling down page
	    driver.findElement(By.cssSelector("div.dropdown.inst-state > div > span > span")).click();
	    Thread.sleep(4000);
	    WebElement element7 = driver.findElement(By.cssSelector(".undefined.undefined.anOption.js-hover-li[data-val='US-VA']"));  
	    new Actions(driver).moveToElement(element7).perform();
	    Thread.sleep(2000);
	    element7.click();
	    Thread.sleep(4000);
	    driver.findElement(By.cssSelector("div.labelHolder.store > div > div > span > span")).click();
	    Thread.sleep(5000);
	    WebElement element8 = driver.findElement(By.cssSelector(".US-VA.anOption[data-val='205']"));  
	    new Actions(driver).moveToElement(element8).perform(); 
	    Thread.sleep(2000);
	    element8.click();
	    Thread.sleep(4000);
	    driver.findElement(By.cssSelector("#checkbox2")).click();
	    driver.findElement(By.cssSelector("#checkbox3")).click();
	    Thread.sleep(2000);
	    driver.findElement(By.cssSelector("#btnnuregisteration")).click();
	    Thread.sleep(3000);

	    // Filling "Tell us about yourself" page
	    driver.findElement(By.cssSelector("#address1")).sendKeys(Address1);
	    driver.findElement(By.cssSelector("#address2")).sendKeys(Address2);
	    WebElement scroll = driver.findElement(By.cssSelector("#city"));  
	    scroll.sendKeys(Keys.PAGE_DOWN); //  ** Scrolling down page
	    Thread.sleep(1000);
	    driver.findElement(By.cssSelector("#city")).sendKeys(City);
	    driver.findElement(By.cssSelector("div.labelHolder.state-drop > div > div > span > span")).click();
	    Thread.sleep(7000);
	    WebElement element9 = driver.findElement(By.cssSelector(".undefined.undefined.anOption[data-val='US-VA']"));  
	    new Actions(driver).moveToElement(element9).perform();
	    Thread.sleep(4000);
	    element9.click();
	    Thread.sleep(5000);
	    driver.findElement(By.cssSelector("#zipCode")).sendKeys(Zip);
	    driver.findElement(By.cssSelector("#phone")).sendKeys(Phone);
	    WebElement scroll3 = driver.findElement(By.cssSelector("#btnSaveAccountNew"));  
	    scroll3.sendKeys(Keys.PAGE_DOWN); //  ** Scrolling down page
	    Thread.sleep(1000);
	    driver.findElement(By.cssSelector("#c0020")).click();
	    driver.findElement(By.cssSelector("#btnSaveAccountNew")).click();
	    Thread.sleep(4000);  
	    Assert.assertEquals(driver.findElements(By.cssSelector(".ahp-heading")).isEmpty(),false, "If Account home doesn't shows up then test will fail");
		}
	}


	
//	@Test (dataProvider = "CheckoutParameters")
//	public void NewUserRegistrationTest (String Location,String StoreName,String PDP,String Quantity,String ShipOption,String FirstName,
//			String LastName,String Company,String Address1,String Address2,String City,String State,String Zip,String Email,String Password,
//			String Phone,String CreditCard,String ExpirationMonth,String ExpirationYear,String CVV)
//					
//					throws InterruptedException, BiffException, IOException {
//		
//		logger=report.startTest("New User Registration using random credentials or credential from DB");
//		
//		//** Random E-mail Creation
//		Random rand = new Random();
//	    int randomNum = rand.nextInt((1000 - 1) + 1) + 1;
//	    int randomNum_2 = rand.nextInt((1000 - 1) + 1) + 1;
//
//		driver.get(ConfigurationFunctions.locationSet+Location);
//		PageLoad(driver); // Will not trigger the next control until loading the page
//		
//		//** By Passing Age Gate and Welcome Modal
//		Checkout.AgeGateWelcome(driver);
//		PageLoad(driver); // Will not trigger the next control until loading the page
//		
//	    // ** Checking for survey pop-up
//	    Checkout.SurverPopup(driver);
//	    driver.findElement(PageGlobal.TopNavAccount).click();
//	    Thread.sleep(2000);
//	    driver.findElement(PageGlobal.SignUp).click();
//	    Thread.sleep(2000);
//	
//	 // ** Filling up "Create your account" page
//	    driver.findElement(By.cssSelector("#firstName")).sendKeys(FirstName);
//	    driver.findElement(By.cssSelector("#lastName")).sendKeys(LastName);
//	    Thread.sleep(2000);
//
//	    // *** Register Email from DB
////	    driver.findElement(By.cssSelector("#email")).sendKeys(Email);
////	    driver.findElement(By.cssSelector("#checkEmail")).sendKeys(Email);
//	    
//	    // *** Register Random Email
//	    driver.findElement(By.cssSelector("#email")).sendKeys("autoemail_"+randomNum+"."+randomNum_2+"@totalwine.com");
//    	String email = driver.findElement(By.cssSelector("#email")).getAttribute("value");
//    	System.out.println("Registered Email Address: "+email);
//	    driver.findElement(By.cssSelector("#pwd")).sendKeys(Password);
//	    driver.findElement(By.cssSelector("#phone")).sendKeys(Phone);
//	    Thread.sleep(2000);
//	    WebElement scroll2 = driver.findElement(By.cssSelector("#btnnuregisteration"));  
//	    scroll2.sendKeys(Keys.PAGE_DOWN); //  ** Scrolling down page
//	    driver.findElement(By.cssSelector("div.dropdown.inst-state > div > span > span")).click();
//	    Thread.sleep(4000);
//	    WebElement element7 = driver.findElement(By.cssSelector(".undefined.undefined.anOption.js-hover-li[data-val='US-VA']"));  
//	    new Actions(driver).moveToElement(element7).perform();
//	    element7.click();
//	    Thread.sleep(4000);
//	    driver.findElement(By.cssSelector("div.labelHolder.store > div > div > span > span")).click();
//	    Thread.sleep(4000);
//	    WebElement element8 = driver.findElement(By.cssSelector(".US-VA.anOption[data-val='205']"));  
//	    new Actions(driver).moveToElement(element8).perform();  
//	    element8.click();
//	    Thread.sleep(4000);
//	    driver.findElement(By.cssSelector("#checkbox2")).click();
//	    driver.findElement(By.cssSelector("#checkbox3")).click();
//	    Thread.sleep(2000);
//	    driver.findElement(By.cssSelector("#btnnuregisteration")).click();
//	    Thread.sleep(3000);
//
//	    // Filling "Tell us about yourself" page
//	    driver.findElement(By.cssSelector("#address1")).sendKeys(Address1);
//	    driver.findElement(By.cssSelector("#address2")).sendKeys(Address2);
//	    WebElement scroll = driver.findElement(By.cssSelector("#city"));  
//	    scroll.sendKeys(Keys.PAGE_DOWN); //  ** Scrolling down page
//	    Thread.sleep(1000);
//	    driver.findElement(By.cssSelector("#city")).sendKeys(City);
//	    driver.findElement(By.cssSelector("div.labelHolder.state-drop > div > div > span > span")).click();
//	    Thread.sleep(4000);
//	    WebElement element9 = driver.findElement(By.cssSelector(".undefined.undefined.anOption[data-val='US-VA']"));  
//	    new Actions(driver).moveToElement(element9).perform();
//	    element9.click();
//	    Thread.sleep(4000);
//	    driver.findElement(By.cssSelector("#zipCode")).sendKeys(Zip);
//	    driver.findElement(By.cssSelector("#phone")).sendKeys(Phone);
//	    WebElement scroll3 = driver.findElement(By.cssSelector("#btnSaveAccountNew"));  
//	    scroll3.sendKeys(Keys.PAGE_DOWN); //  ** Scrolling down page
//	    Thread.sleep(1000);
//	    driver.findElement(By.cssSelector("#c0020")).click();
//	    driver.findElement(By.cssSelector("#btnSaveAccountNew")).click();
//	    Thread.sleep(4000);  
//	    Assert.assertEquals(driver.findElements(By.cssSelector(".ahp-heading")).isEmpty(),false, "If Account home doesn't shows up then test will fail");
//		}
//	}