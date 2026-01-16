package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class Practice_Frame {

	  public static void main(String[] args) throws InterruptedException {
		System.setProperty("webdriver.chrome.driver","" +"drivers\\chromedriver.exe");
        WebDriver driver=new ChromeDriver();
        driver.get("https://demoqa.com/frames");
     //   driver.manage().window().maximize();
        
        
        
        
        
        

      //  WebElement frame1 =driver.findElement(By.xpath("(//*[@src='/sample'])[1]"));
//        driver.switchTo().frame(0);
//       WebElement sampleHeading =driver.findElement(By.xpath("//*[@id='sampleHeading']"));
//       String text1 =sampleHeading.getText();
//       
//       System.out.println("text1");
//       


	  
	  
	  
      //Switch to Frame using Index
      Thread.sleep(1000);
        driver.switchTo().frame(0);
        
        //Identifying the heading in webelement
        WebElement frame1Heading= driver.findElement(By.id("sampleHeading"));
        
        //Finding the text of the heading
        String frame1Text=frame1Heading.getText();
        
        //Print the heading text
        System.out.println(frame1Text);
        
	  }
        
        
}
	
	
	
	
