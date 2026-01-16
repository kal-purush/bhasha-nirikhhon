package ObjectRepository;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class DashboardPage {
	private static WebElement element = null;

	public static WebElement btnSignout(WebDriver driver)
	{
		element=driver.findElement(By.xpath("/html/body/header/div/div[1]/div/div[1]/ul/li[3]/a"));
		return element;
	}
	
	public static WebElement PlaceOrder(WebDriver driver)
	{
		element=driver.findElement(By.xpath("/html/body/div[1]/section/div[2]/div/div[2]/div[1]/div[1]/ul/div[1]/ul/li[1]/div/div/a[1]"));
		return element;
	}
	public static WebElement NewWorkCompOrder(WebDriver driver)
	{
		element=driver.findElement(By.xpath("/html/body/div[1]/section/div[2]/div/div[2]/div[1]/div[1]/ul/div[1]/ul/li[1]/div/div/ul/li[2]/a"));
		return element;
	}
	public static WebElement NewCivilOrder(WebDriver driver)
	{
		element=driver.findElement(By.xpath("/html/body/div[1]/section/div[2]/div/div[2]/div[1]/div[1]/ul/div[1]/ul/li[1]/div/div/ul/li[3]/a"));
		return element;
	}	
	
	
}