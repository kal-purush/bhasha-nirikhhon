package com.totalwine.test.global;

/*
 * PDP Tabs Workflow
 * Workflow:
 * 	1. Browse to a PDP
 * 	  a. Validate the contents of each tab
 *  2. Iterate through the workflow for each item type (Wine, Beer, Spirits, Accessory, Cigar)
 *  
 * Technical Modules:
 * 	1. BeforeMethod (Test Pre-requisites):
 * 			Invoke webdriver
 * 			Maximize browser window
 * 	2. Test (Workflow)
 * 	3. AfterMethod
 * 			Take screenshot, in case of failure
 * 			Close webdriver
 * 	4. AfterClass
 * 			Quit webdriver
 */

import java.io.IOException;

import jxl.read.biff.BiffException;

import org.testng.*;
import org.testng.annotations.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.testng.annotations.BeforeMethod;
import com.totalwine.test.config.ConfigurationFunctions;
import com.totalwine.test.trials.Browser;

public class BrowseOnly extends Browser {
	
	private String IP="71.193.51.0";

	@BeforeMethod
	  public void setUp() throws Exception {
	    driver.manage().window().maximize();
	  }  
	
	@Test 
	public void PDPTest () throws InterruptedException, BiffException, IOException {
		
		driver.get(ConfigurationFunctions.locationSet+IP);
		Thread.sleep(5000);
		driver.findElement(By.id("btnYes")).click();
		Thread.sleep(5000);
	    driver.findElement(By.cssSelector("#email-signup-overlay-new-site > div.modal-dialog > div.modal-content > div.modal-body > p.close > a.btn-close")).click();
	    Thread.sleep(5000);
	    
		Actions action=new Actions(driver);
		
		//Browse to PLP
		WebElement toplevelnav = driver.findElement(By.xpath("//a[contains(@href,'/c/c0020')]")); 
		action.moveToElement(toplevelnav).build().perform(); //Top Level Menu Hover
		WebElement plpnav=driver.findElement(By.xpath("//a[contains(@href,'/c/013005')]"));
		JavascriptExecutor js = (JavascriptExecutor)driver;
		js.executeScript("arguments[0].click();", plpnav);
		Thread.sleep(5000);
		
		//Access the PDP
		WebElement plpmove = driver.findElement(By.cssSelector("ul.header-classes")); //Moving the mouse away from the top level menu 
		action.moveToElement(plpmove).build().perform();
		String winename = driver.findElement(By.cssSelector("a.analyticsProductName")).getText();
		System.out.println(winename);
		driver.findElement(By.cssSelector("a.analyticsProductName")).click(); //Click the first item link in the PLP
		Thread.sleep(5000);

		//PDP Tab 1
		Assert.assertEquals(driver.findElements(By.cssSelector("section.pdp-tab-overview-prod-img > div.pdp-tab-overview-prod-img-bottle-img.pdp-img-zoom-modal-zoom-reset > img.anPDPImage")).isEmpty(),false);
	    Assert.assertEquals(driver.findElements(By.cssSelector("h1.product-name")).isEmpty(),false);
	    Assert.assertEquals(driver.findElements(By.cssSelector("div.pdp-tab-overview-desc-name > div.wine_details > ul.wine-right-details > li > h2 > a.analyticsCountryState")).isEmpty(),false);
	    Assert.assertEquals(driver.findElements(By.cssSelector("span.pdp-tabs-ind-left")).isEmpty(),false);
	    Assert.assertEquals(driver.findElements(By.cssSelector("img.anPDPImage.active")).isEmpty(),false);
	    Assert.assertEquals(driver.findElements(By.cssSelector("div.breadcrumbs")).isEmpty(),false);
	    Assert.assertEquals(driver.findElements(By.cssSelector("span.tabs-right.anPDPTab")).isEmpty(),false);
	}
}