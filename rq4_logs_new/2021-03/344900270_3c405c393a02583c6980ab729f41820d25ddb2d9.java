import java.util.List;
import org.openqa.selenium.By;
//import org.openqa.selenium.xpath;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;


import java.net.MalformedURLException;
import java.net.URL;



//import org.junit.Assert;
//import org.testing.Assert;
import org.openqa.selenium.Platform;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
public class basicxpath {

	public static void main(String[] args) {
		System.setProperty("webdriver.chrome.driver","D:\\SELVA\\chrome\\chromedriver.exe");
		WebDriver driver = new ChromeDriver();
		driver.get("https://en.wikipedia.org/wiki/wikipedia");
		driver.findElement(By.xpath("//a[text()='Create account']")).click();
		driver.findElement(By.xpath("//input[@id='wpName2']")).sendKeys("Ashokkumar");
		driver.findElement(By.xpath("//input[@id='wpPassword2']")).sendKeys("AshokRajesh");
		// my comment
	}
}
		// TODO Auto-generated method stu