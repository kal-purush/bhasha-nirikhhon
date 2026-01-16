package pages;


import java.util.ArrayList;
import java.util.Set;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;


public class Practice_demo {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("webdriver.chrome.driver", ""+ "drivers\\chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.manage().window().maximize();
        driver.get("https://demoqa.com/browser-windows");

       
  // Opening all the child window
        
       Thread.sleep(1000);
        WebElement newtab=driver.findElement(By.id("tabButton"));
        WebElement newwindow=driver.findElement(By.xpath("//button[text()='New Window']"));  
        WebElement newwindowmsg=driver.findElement(By.xpath("//button[text()='New Window Message']"));
 
        System.out.println(newtab);              newtab.click();
        System.out.println(newwindow);           newwindow.click();
        System.out.println(newwindowmsg);        newwindowmsg.click();
      
        Set<String> set=driver.getWindowHandles();       System.out.println(set);
        ArrayList<String>li=new ArrayList<String>(set);  System.out.println(li);
        
        
        
                Thread.sleep(1000);
String data3=   driver.switchTo().window(li.get(3)).findElement(By.xpath("//body")).getText();;
                System.out.println(data3);
                driver.close();
        
        
                Thread.sleep(1000);      
String data2=   driver.switchTo().window(li.get(2)).findElement(By.xpath("//h1[@id='sampleHeading']")).getText();
                System.out.println(data2);
                driver.close();
        

                Thread.sleep(1000);
String data1=   driver.switchTo().window(li.get(1)).findElement(By.xpath("//*[text()='This is a sample page']")).getText();
                System.out.println(data1);
                driver.close();

                Thread.sleep(1000);
                driver.switchTo().window(li.get(0)); 
                driver.close();
        }
    }