package day1;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import io.github.bonigarcia.wdm.WebDriverManager;

public class JavaExecutor {
public static void main(String[] args) {
		
		WebDriverManager.chromedriver().setup();
		ChromeDriver driver = new ChromeDriver();
        driver.get("https://en-gb.facebook.com");
        driver.manage().window().maximize();
		WebElement txtUserName = driver.findElement(By.id("email"));
		JavascriptExecutor js =(JavascriptExecutor)driver;
		js.executeScript("arguments[0].setAtrribute('value','Green')", txtUserName);
		Object obj =  js.executeScript("return arguments[0].getAttribute('value')", txtUserName);
		String name =(String)obj;
		System.out.println(name); 
		WebElement txtPassword = driver.findElement(By.id("pass"));
		js.executeScript("arguments[0].setAtrribute('value','lura')", txtPassword);
		Object obj1 =  js.executeScript("return arguments[0].getAttribute('value')", txtPassword);
		String name1 =(String)obj1;
		System.out.println(name1); 
		}
}