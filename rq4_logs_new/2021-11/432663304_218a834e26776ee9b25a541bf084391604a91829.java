package seleniumBootcamp;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.testng.annotations.BeforeMethod;

import io.github.bonigarcia.wdm.WebDriverManager;

public class Baselogin {
	WebDriver driver;
	@BeforeMethod
	public void login() {
	ChromeOptions options = new ChromeOptions();
	options.addArguments("--disable-notifications");

	WebDriverManager.chromedriver().setup();
	driver = new ChromeDriver(options);

	driver.get("https://d2w00000dnnegeal-dev-ed.my.salesforce.com/");
	driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
	driver.manage().window().maximize();
	driver.findElement(By.id("username")).sendKeys("makaia@testleaf.com");
	driver.findElement(By.id("password")).sendKeys("SelBootcamp$1234");
	driver.findElement(By.id("Login")).click();
	}
}