package com.paytm.bus;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class PageTest2 {
	static WebDriver driver= new FirefoxDriver();
	public static void selectTravelDate(int day,String month,String year) throws InterruptedException{
		WebElement trvDay=driver.findElement(By.xpath("//label[text()='Select Date']/following-sibling::input[1]"));
		WebElement trvMonth=driver.findElement(By.xpath("//div[@class='picker__month']"));
		WebElement trvYear=driver.findElement(By.xpath("//div[@class='picker__year']"));
		WebElement nxtButton=driver.findElement(By.xpath("//div[@class='picker__nav--next' and @data-nav='1']"));
		
		
		driver.findElement(By.xpath("//label[text()='Select Date']/following-sibling::input[1]")).click();

		if(trvYear.getText().contains(year)){
			if(trvMonth.getText().contains(month)){
				driver.findElement(By.xpath("//table//div[text()='"+day+"' and contains(normalize-space(@class),'infocus')]")).click();

			} else {
				nxtButton.click();
				Thread.sleep(2000);
			}
			
		
		}

	}
	
	public static void main(String[] args){
		driver.get("http://paytm.com");
		driver.manage().window().maximize();
		driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
		
		driver.findElement(By.xpath("//a[text()='Bus Tickets']")).click();
		try {
			selectTravelDate(10,"May","2016");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}