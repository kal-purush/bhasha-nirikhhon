package assignments;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class Assignment1 {
	
	WebDriver driver;
	String errorMessage="Invalid credentials";
	
	//Test Case 1: Wrong Username and wrong password
	void TestCase1() throws InterruptedException
	{
		driver=new ChromeDriver();
		driver.get("https://opensource-demo.orangehrmlive.com/");
		Thread.sleep(2000);
		driver.manage().window().maximize();
		Thread.sleep(1000);
		driver.findElement(By.xpath("//input[@placeholder='Username']")).sendKeys("user");
		driver.findElement(By.xpath("//input[contains(@name,'pass')]")).sendKeys("user123");
		driver.findElement(By.xpath("//button[@type='submit']")).click();
		Thread.sleep(2000);
		WebElement element=driver.findElement(By.xpath("//p[@class='oxd-text oxd-text--p oxd-alert-content-text']"));
		String text=element.getText();
		
		if(text.equals(errorMessage))
		{
			System.out.println("Valid error message");
		}else
		{
			System.out.println("Error message is not as expected");
		}
	}
	
	//Test Case 2: Correct Username and wrong password
	void TestCase2() throws InterruptedException
	{
		driver=new ChromeDriver();
		driver.get("https://opensource-demo.orangehrmlive.com/");
		Thread.sleep(2000);
		driver.manage().window().maximize();
		Thread.sleep(1000);
		driver.findElement(By.xpath("//input[@name='username'and @placeholder='Username']")).sendKeys("Admin");		
		driver.findElement(By.cssSelector("input.oxd-input--active[placeholder=Password]")).sendKeys("user123");
		driver.findElement(By.cssSelector("button.orangehrm-login-button")).click();
		Thread.sleep(1000);
		WebElement element=driver.findElement(By.xpath("//p[@class='oxd-text oxd-text--p oxd-alert-content-text']"));
		String text=element.getText();
		
		if(text.equals(errorMessage))
		{
			System.out.println("Valid error message");
		}else
		{
			System.out.println("Error message is not as expected");
		}
			
	}
	
	//Test Case 3: Wrong Username and correct password	
	
	void TestCase3() throws InterruptedException
	{
		driver=new ChromeDriver();
		driver.get("https://opensource-demo.orangehrmlive.com/");
		Thread.sleep(2000);
		driver.manage().window().maximize();
		Thread.sleep(1000);
		driver.findElement(By.cssSelector("input[name=username]")).sendKeys("user");
		driver.findElement(By.name("password")).sendKeys("admin123");
		driver.findElement(By.cssSelector("button.orangehrm-login-button")).click();
		Thread.sleep(1000);
				
		WebElement element=driver.findElement(By.xpath("//p[@class='oxd-text oxd-text--p oxd-alert-content-text']"));
		String text=element.getText();
		
		if(text.equals(errorMessage))
		{
			System.out.println("Valid error message");
		}else
		{
			System.out.println("Error message is not as expected");
		}
		
	}
	
	// Test Case 4: correct Username and correct password
	void TestCase4() throws InterruptedException
	{
		driver=new ChromeDriver();
		driver.get("https://opensource-demo.orangehrmlive.com/");
		Thread.sleep(2000);
		driver.manage().window().maximize();
		Thread.sleep(1000);
		driver.findElement(By.xpath("//input[@name='username'and @placeholder='Username']")).sendKeys("Admin");		
		driver.findElement(By.cssSelector("input.oxd-input--active[placeholder=Password]")).sendKeys("admin123");
		driver.findElement(By.cssSelector("button.orangehrm-login-button")).click();
		System.out.println("Login successfully");
		Thread.sleep(2000);
		String title=driver.getTitle();
		System.out.println("Title of the page: "+title);
		//driver.findElement(By.xpath("//i[@class='oxd-icon bi-caret-down-fill oxd-userdropdown-icon']")).click();
		driver.findElement(By.cssSelector(".oxd-userdropdown-icon")).click();
		Thread.sleep(1000);
		driver.findElement(By.linkText("Logout")).click();
		System.out.println("Logout successfully");			
	}
	
	
}