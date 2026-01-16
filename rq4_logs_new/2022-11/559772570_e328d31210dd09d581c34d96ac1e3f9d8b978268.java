package Automationtest;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class flipkart 
{
	public static WebDriver oBrowser=null;

	public static void main(String[] args) 
	{
		launchbrowser();
		navigate();

	}
	public static void launchbrowser()
	{
		try
		{
			System.setProperty("webdriver.chrome.driver","C:\\ExampleAtomation\\Automation\\Web'Autometion\\Library\\drivers\\chromedriver.exe");
				
		oBrowser=new ChromeDriver();	
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	private static void navigate()
	{
		try
		{
		oBrowser.get("https://www.w3schools.com\r\n");
		Thread.sleep(2000);
		oBrowser.findElement(By.id("navbtn_tutorials")).click();
		List<WebElement> olinks=oBrowser.findElements(By.xpath("//a[@href]"));
		int linkcount=olinks.size();
		System.out.println( "number of links:"+linkcount);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}