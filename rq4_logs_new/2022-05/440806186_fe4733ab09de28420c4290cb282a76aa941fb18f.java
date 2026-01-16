package newAssignmentWeek4;

import java.time.Duration;

import java.util.List;
import java.util.ArrayList;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import java.util.LinkedHashSet;
import java.util.Set;

import io.github.bonigarcia.wdm.WebDriverManager;

public class Chittorgarh {

	public static void main(String[] args) {
		//Assignment1:
		WebDriverManager.chromedriver().setup();
				ChromeDriver driver = new ChromeDriver();
				driver.manage().window().maximize();
				driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(20));
				
				  
			//1. Launch the URL https://www.chittorgarh.com/
				driver.get("https://www.chittorgarh.com/");
			//2. Click on stock market
			driver.findElement(By.xpath("//a[text()='STOCK MARKET ']")).click();
			//3. Click on NSE bulk Deals
			driver.findElement(By.xpath("(//a[text()='NSE Bulk Deals'])[1]")).click();
			//4. Get all the Security names
			List<WebElement> securityNames = driver.findElements(By.xpath("//table[@class='table table-bordered table-condensed table-striped']//tr"));
			
			List<String>lstSecurity=new ArrayList<String>();
			
			for (int i = 1; i < securityNames.size(); i++) {
				String names = driver.findElement(By.xpath("//table[@class='table table-bordered table-condensed table-striped']//tr["+i+"]/td[3]")).getText();
				lstSecurity.add(names);
				//Print the Security names
				System.out.println(names);
				}
			System.out.println("List"+lstSecurity.size());
			//get the list into set
			Set<String>setSecurity=new LinkedHashSet<String>(lstSecurity);
			//to find the size
			System.out.println(setSecurity.size());
			//5. Ensure whether there are duplicate Security names		
			if(lstSecurity.size()==setSecurity.size()) {
						System.out.println("No Duplicate Security names  ");
					}
					else {
						System.out.println("Duplicate Security names Available");
					}
					
				}
	
			
			
	
	}

