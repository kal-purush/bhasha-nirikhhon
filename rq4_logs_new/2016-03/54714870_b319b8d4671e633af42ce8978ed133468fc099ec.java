package chrome;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

public class PageVisitor {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		System.setProperty("webdriver.chrome.driver", 
				new java.io.File("driver/chromedriver").getAbsolutePath());
		WebDriver driver = new ChromeDriver();
		
		//driver.manage().window().maximize();
		
		driver.get("https://www.google.com/ncr");

		driver.findElement(By.id("lst-ib")).sendKeys("AdNauseam");

		Thread.sleep(1500);
		System.out.println(driver.getTitle());
		System.out.println(driver.findElement(By.id("resultStats")).getText());
		driver.close();

	}

}