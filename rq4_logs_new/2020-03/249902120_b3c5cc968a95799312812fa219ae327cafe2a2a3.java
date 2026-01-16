package day1;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.interactions.Actions;

public class SecondSeleniumScriptv2 {

	public static void main(String[] args) throws InterruptedException {
		
		System.setProperty("webdriver.chrome.driver", "C:\\Users\\varshinik\\Documents\\Selenium Drivers\\chromedriver_win32\\chromedriver.exe");
		WebDriver driver  = new ChromeDriver();
	    driver.manage().timeouts().implicitlyWait(15,TimeUnit.SECONDS);
	    
	    //Open the web app
	    driver.navigate().to("https://amazon.in");
	    driver.manage().window().maximize();
	    String title = driver.getTitle();
	    
	    
	    if(title.equalsIgnoreCase("Amazon.in"))
	    	System.out.println("Title matches");
	    else
	    	System.out.println(title);
	    		
	
	    //locate a Web element
		//String tagname = driver.findElement(By.cssSelector("#nav-orders")).getText();
		//System.out.println(tagname);
		//Thread.sleep(3000);
		
	    //locate a Web element
		String tagname = driver.findElement(By.partialLinkText("Best Sellers")).getText();
		System.out.println(tagname);
		Thread.sleep(3000);
		
		
		
		//Drop down
		driver.findElement(By.partialLinkText("Best Sellers")).click();
		Thread.sleep(3000);
		
		
		//click on first best seller
		//driver.findElement(By.cssSelector("#a-meter-animate")).click();
		//Thread.sleep(3000); 
		//Drop down
		//WebElement category = driver.findElement(By.cssSelector("#nav-orders"));
		//Actions action =  new Actions(driver);
		//action.moveToElement(category).perform();
		//Thread.sleep(3000);
	    
		driver.close();
		driver.quit();
	}
}