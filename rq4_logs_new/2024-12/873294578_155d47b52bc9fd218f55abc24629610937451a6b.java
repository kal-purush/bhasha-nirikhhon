package com.mayab.quality.functional;

import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;
import org.junit.*;
import static org.junit.Assert.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.Select;

import io.github.bonigarcia.wdm.WebDriverManager;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.time.Duration;

public class SeleniumTest2 {
  private WebDriver driver;
  private String baseUrl;
  private boolean acceptNextAlert = true;
  private StringBuffer verificationErrors = new StringBuffer();
  JavascriptExecutor js;
  @Before
  public void setUp() throws Exception {
    WebDriverManager.chromedriver().setup();
    driver = new ChromeDriver();
    baseUrl = "https://www.google.com/";
    driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(60));
    js = (JavascriptExecutor) driver;

  }

  @Test
  public void testUntitledTestCase() throws Exception {
	pause(5000);
    driver.get(baseUrl + "chrome://newtab/");
    pause(5000);
    driver.get("https://www.google.ch/");
    pause(5000);
    driver.get("https://en.wikipedia.org/wiki/Puppy");
    pause(5000);
    
    WebElement actualResult = driver.findElement(By.xpath("/html/body/div[2]/div/div[3]/main/div[3]/div[1]/div[2]"));
    assertThat(actualResult,is("From Wikipedia, the free encyclopedia"));
    
  //  assertTrue(driver.findElement(By.cssSelector("BODY")).getText().matches("^[\\s\\S]*From Wikipedia, the free encyclopedia[\\s\\S]*$"));
  }
  
  private void pause(long mils) {
	  try {
		  Thread.sleep(mils);
		  
	  }catch(Exception e) {
		  e.printStackTrace();
	  }
  }

  @After
  public void tearDown() throws Exception {
    driver.quit();
    String verificationErrorString = verificationErrors.toString();
    if (!"".equals(verificationErrorString)) {
      fail(verificationErrorString);
    }
  }

  private boolean isElementPresent(By by) {
    try {
      driver.findElement(by);
      return true;
    } catch (NoSuchElementException e) {
      return false;
    }
  }

  private boolean isAlertPresent() {
    try {
      driver.switchTo().alert();
      return true;
    } catch (NoAlertPresentException e) {
      return false;
    }
  }

  private String closeAlertAndGetItsText() {
    try {
      Alert alert = driver.switchTo().alert();
      String alertText = alert.getText();
      if (acceptNextAlert) {
        alert.accept();
      } else {
        alert.dismiss();
      }
      return alertText;
    } finally {
      acceptNextAlert = true;
    }
  }
}