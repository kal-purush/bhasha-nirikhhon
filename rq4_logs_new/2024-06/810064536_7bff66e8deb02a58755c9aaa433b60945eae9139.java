package learningpkg;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import java.util.Scanner;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.testng.annotations.AfterClass;


public class Locators {
	WebDriver driver;
  @Test
  public void openUrl() {
	  driver.get("https://opensource-demo.orangehrmlive.com/web/index.php/auth/login");
  }
  @BeforeMethod
  public void beforeMethod() {
	/*
	 * Scanner sc = new Scanner(System.in);
	 * System.out.println("Enter the name of Browser"); String browserName =
	 * sc.nextLine();
	 * 
	 * if(browserName.equals("firefox")) { driver = new FirefoxDriver(); }else if
	 * (browserName.equals("edge")){ driver = new EdgeDriver(); } else { driver =
	 * new ChromeDriver(); } sc.close(); }
	 */
	  driver = new ChromeDriver();
  }

  @AfterMethod	
  public void afterMethod() {
	//  driver.quit();
  }

  @BeforeClass
  public void beforeClass() {
  }

  @AfterClass
  public void afterClass() {
  }

}