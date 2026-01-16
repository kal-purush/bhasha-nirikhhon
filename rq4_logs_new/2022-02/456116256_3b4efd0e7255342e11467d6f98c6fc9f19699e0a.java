import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.AfterTest;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.openqa.selenium.By;

//declare Selenium WebDriver
public class SeleniumTest {
//declare Selenium WebDriver	
private WebDriver webDriver;
  @Test
  public void f() {
  }
  @Test  
  public void checkUrl() {
	  //Load website as a new page
	  webDriver.navigate().to("http://localhost:8090/ProjectBryan/FeedbackListServlet/dashboard");
	  
	  //Assert the Url to check that we are indeed in the correct website
	  Assert.assertEquals(webDriver.getCurrentUrl(), "http://localhost:8090/ProjectBryan/FeedbackListServlet/dashboard");
	  
	  System.out.println("Url: "+webDriver.getCurrentUrl());
  }
  @Test
  public void checkDashboardTitle() {
	  //Load website as a new page
	  webDriver.navigate().to("http://localhost:8090/ProjectBryan/FeedbackListServlet/dashboard");
	  
	  //Assert the title to check that we are indeed in the correct website
	  Assert.assertEquals(webDriver.getTitle(), "Feedback Platform");
	  
	  System.out.println("Dashboard Title: "+webDriver.getTitle());

  }
  
  @Test
  public void checkCreateTitle() {
	  //Load website as a new page
	  webDriver.navigate().to("http://localhost:8090/ProjectBryan/FeedbackListServlet/dashboard");
	  
	  //Retrieve link using it's name and click on it
	  webDriver.findElement(By.linkText("Add New Feedback")).click();

	  //Assert the new title to check that the title contain Wikipedia and the button had successfully bring us to the new page
	  Assert.assertTrue(webDriver.getTitle().contains("Create Feedback"));
	  System.out.println("Create Title: "+webDriver.getTitle());
  }
  
  @Test
  public void checkEditTitle() {
	  //Load website as a new page
	  webDriver.navigate().to("http://localhost:8090/ProjectBryan/FeedbackListServlet/dashboard");
	  
	  //Retrieve link using it's name and click on it
	  webDriver.findElement(By.linkText("Edit")).click();

	  //Assert the new title to check that the title contain Wikipedia and the button had successfully bring us to the new page
	  Assert.assertTrue(webDriver.getTitle().contains("Feedback Edit"));
	  System.out.println("Edit Title: "+webDriver.getTitle());
  }
  
  @BeforeTest
  public void beforeTest() {
	  
	  //Setting system properties of ChromeDriver
	  //to amend directory path base on your local file path
	  String chromeDriverDir = "C:\\Program Files\\Google\\Chrome\\chromedriver.exe";

	  System.setProperty("webdriver.chrome.driver", chromeDriverDir);

	  //initialize FirefoxDriver at the start of test
	  webDriver = new ChromeDriver();  
  }

  @AfterTest
  public void afterTest() {
	  //Quit the ChromeDriver and close all associated window at the end of test
	  webDriver.quit();	
  }

}