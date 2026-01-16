package Codingii;

import java.time.Duration;
import java.util.Set;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.chrome.ChromeDriver;

import io.github.bonigarcia.wdm.WebDriverManager;

public class mergeContact2 {

	public static void main(String[] args) {
		WebDriverManager.chromedriver().setup();
	    ChromeDriver driver = new ChromeDriver();
	    driver.manage().window().maximize();
	    driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(30));
	    driver.get("http://leaftaps.com/opentaps/control/login");
	    driver.manage().window().maximize();
		driver.findElement(By.id("username")).sendKeys("DemoSalesManager");
		driver.findElement(By.id("password")).sendKeys("crmsfa");
		driver.findElement(By.className("decorativeSubmit")).click();
		driver.findElement(By.linkText("CRM/SFA")).click();
		driver.findElement(By.linkText("Contacts")).click();
		driver.findElement(By.linkText("Merge Contacts")).click();
		String oldWindow = driver.getWindowHandle();
		driver.findElement(By.xpath("(//img[@src='/images/fieldlookup.gif'])[1]")).click();
        Set<String> windowHandles = driver.getWindowHandles();
        for (String newWindow : windowHandles) {
        	driver.switchTo().window(newWindow);
			
		}
        driver.findElement(By.xpath("(//a[@class='linktext'])[1]")).click();
        
        driver.switchTo().window(oldWindow);
        driver.findElement(By.xpath("(//img[@src='/images/fieldlookup.gif'])[2]")).click();
        Set<String> windowHandles2 = driver.getWindowHandles();
        for (String newWindow1 : windowHandles2) {
        	driver.switchTo().window(newWindow1);
			
		}
        driver.findElement(By.xpath("(//a[@class='linktext'])[5]")).click();
        driver.switchTo().window(oldWindow);
        driver.findElement(By.xpath("//a[text()='Merge']")).click();
        Alert alert=driver.switchTo().alert();
        alert.accept();
       System.out.println(driver.getTitle());
       
	}

	
}