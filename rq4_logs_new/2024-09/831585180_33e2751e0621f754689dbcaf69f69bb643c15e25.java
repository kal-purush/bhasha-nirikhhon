package DataProvider;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.annotations.Test;

import io.github.bonigarcia.wdm.WebDriverManager;

public class DataProvider_TestFire {
	@Test(dataProvider = "Sumit",dataProviderClass=DataProvider_in_testNG_2D_Arrays.class )
	public void datatestfire (String uname,String pwd) throws InterruptedException{
	 WebDriverManager.chromedriver().setup();
		WebDriver driver=new ChromeDriver();
		driver.get("http://testfire.net/");
		Thread.sleep(2000);
		driver.findElement(By.id("LoginLink")).click();
		Thread.sleep(2000);
		driver.findElement(By.id("uid")).sendKeys(uname);
		driver.findElement(By.id("passw")).sendKeys(pwd);
		Thread.sleep(2000);
		driver.findElement(By.name("btnSubmit")).click();
		Thread.sleep(2000);
		driver.close();

	}
	}