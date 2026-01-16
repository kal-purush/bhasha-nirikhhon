package com.uiElemints;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class Handlingcssorxpath {

	public static void main(String[] args) {
		System.setProperty("webdriver.chrome.driver","/Users/anilkumarreddy/Documents/chrome driver/chromedriver");
		WebDriver driver=new ChromeDriver();
		
		driver.get("https://echoecho.com/htmlforms09.htm");
		List<WebElement> cb= driver.findElements(By.xpath("//input[starts-with(@name,'option')]"));
		
	int i=1;
	
	for(WebElement w : cb)
	{
		if(w.isEnabled()) {
			System.out.println("defaultstatus of the checkbox" +i+"is"+w.isSelected());
			w.click();
			System.out.println("after clickstatus of the checkbox" +i+"is"+w.isSelected());
		}
		i++;
	}
	}

}