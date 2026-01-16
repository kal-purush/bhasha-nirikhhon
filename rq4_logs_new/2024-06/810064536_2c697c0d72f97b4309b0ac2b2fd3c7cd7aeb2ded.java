package assignments;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;


public class LoginTestcases {
WebDriver driver;  
String errorMessage="Invalid credentials";
	@Test (enabled=true)
	///Test Case 1: Wrong Username and wrong password
  public void TestCas1() throws InterruptedException {
		
		driver.findElement(By.name("username")).sendKeys("user");
		driver.findElement(By.cssSelector("input[placeholder=Password]")).sendKeys("user123");
		driver.findElement(By.cssSelector("button.orangehrm-login-button")).click();
		Thread.sleep(1000);
		String invalidLogin= driver.findElement(By.cssSelector("p.oxd-alert-content-text")).getText();
		//System.out.println(invalidLogin);
		if (invalidLogin.equals(errorMessage))
		{
			System.out.println("Valid Error Message");
		}else
		{
			System.out.println("Incorrect error message");
		}
  }
	@Test (enabled=true)
	///Test Case 2: Correct Username and wrong password
  public void TestCas2() throws InterruptedException {
		
		driver.findElement(By.xpath("//input[@name='username']")).sendKeys("Admin");
		driver.findElement(By.xpath("//input[@placeholder='Password']")).sendKeys("user123");
		driver.findElement(By.xpath("//button[@type='submit']")).click();	
		Thread.sleep(2000);
		String invalidLogin= driver.findElement(By.xpath("//p[@class='oxd-text oxd-text--p oxd-alert-content-text']")).getText();
		if (invalidLogin.equals(errorMessage))
		{
			System.out.println("Valid Error Message");
		}else
		{
			System.out.println("Incorrect error message");
		}
   }
	@Test (enabled =true)
	///Test Case 3: Wrong Username and correct password
  public void TestCas3() throws InterruptedException {
	
		driver.findElement(By.xpath("//input[contains(@name,'user')]")).sendKeys("user");
		driver.findElement(By.xpath("//input[contains(@name,'word')]")).sendKeys("admin123");
		driver.findElement(By.xpath("//button[contains(@class,'orangehrm-login-button')]")).click();
		Thread.sleep(2000);
		String invalidLogin= driver.findElement(By.xpath("//p[text()='Invalid credentials']")).getText();
		if (invalidLogin.equals(errorMessage))
		{
			System.out.println("Valid Error Message");
		}else
		{
			System.out.println("Incorrect error message");
		}
   }
	@Test
	///Test Case 4: correct Username and correct password
  public void TestCas4() throws InterruptedException {
	
		driver.findElement(By.xpath("//input[contains(@name,'user')]")).sendKeys("Admin");
		driver.findElement(By.xpath("//input[contains(@name,'word')]")).sendKeys("admin123");
		driver.findElement(By.xpath("//button[contains(@class,'orangehrm-login-button')]")).click();
		System.out.println("Login success");
		//String pageTitle=driver.getTitle();
		System.out.println("Title of the Page: "+driver.getTitle());
		Thread.sleep(2000);
		driver.findElement(By.xpath("//p[@class='oxd-userdropdown-name']")).click();
		Thread.sleep(2000);
		driver.findElement(By.linkText("Logout")).click();
		System.out.println("Logout success");
   }
  @BeforeMethod
  public void beforeMethod() throws InterruptedException {
	  
		driver=new ChromeDriver();
		driver.get("https://opensource-demo.orangehrmlive.com/");
		Thread.sleep(2000);
		driver.manage().window().maximize();
		Thread.sleep(2000);
  }

  @AfterMethod
  public void afterMethod() {
	  
	  driver.quit();
  }

 


}