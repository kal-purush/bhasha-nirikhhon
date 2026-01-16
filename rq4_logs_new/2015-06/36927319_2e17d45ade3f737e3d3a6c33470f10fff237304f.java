
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import org.openqa.selenium.firefox.FirefoxDriver;

public class seleniumsample {


	public static void main(String[] args) {
		
        WebDriver driver = new FirefoxDriver();

        String appUrl = "https://eureka.e-navik.com/";


        driver.get(appUrl);


        driver.manage().window().maximize();


        String expectedTitle = " Sign in - Eureka ";


        String actualTitle = driver.getTitle();

        

// compare the expected title of the page with the actual title of the page and print the result

        if (expectedTitle.equals(actualTitle))

        {
           System.out.println("Verification Successful - The correct title is displayed on the web page.");

        }

       else

        {

               System.out.println("Verification Failed - An incorrect title is displayed on the web page.");

        }


        WebElement username = driver.findElement(By.id("UserLogin_username"));
        username.clear();
        username.sendKeys("pradeep_pandey");
        
        
        
        driver.manage().timeouts().implicitlyWait(60, TimeUnit.SECONDS);
        
	        
	       WebElement pass = driver.findElement(By.id("UserLogin_password"));
	       pass.clear();
	       pass.sendKeys("12345");


       
       
        driver.findElement(By.name("yt0")).click();
      

        

        driver.close();
        System.out.println("Test script executed successfully.");

        


        System.exit(0);

 }


}