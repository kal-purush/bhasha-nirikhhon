package pages;

import java.util.ArrayList;
import java.util.Set;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class Practice_demo3 {

	 public static void main(String[] args) throws InterruptedException {
	        System.setProperty("webdriver.chrome.driver", ""+ "drivers\\chromedriver.exe");
	        WebDriver driver = new ChromeDriver();
	        driver.manage().window().maximize();
	        driver.get("https://demoqa.com/browser-windows");

	       
	  // Opening all the child window
	        
	       Thread.sleep(1000);
	        WebElement newtab1=driver.findElement(By.id("tabButton"));
	        WebElement newtab2=driver.findElement(By.id("tabButton"));
	        WebElement newtab3=driver.findElement(By.id("tabButton"));
	        WebElement newtab4=driver.findElement(By.id("tabButton"));
	   	 
	        System.out.println(newtab1);              newtab1.click();
	        System.out.println(newtab2);              newtab2.click();
	        System.out.println(newtab3);              newtab3.click();
	      
	        
	        Set<String> set=driver.getWindowHandles();       System.out.println(set);
	        ArrayList<String>li=new ArrayList<String>(set);  System.out.println(li);

	        
	        Thread.sleep(1000);
	    	//String data1=   driver.switchTo().window(li.get(3)).findElement(By.xpath("//*[text()='This is a sample page']")).getText();
	    	//System.out.println(data1);
	    	driver.switchTo().window(li.get(3));
	    	driver.close();

	    	                Thread.sleep(1000);
	    	            	String data2=   driver.switchTo().window(li.get(2)).findElement(By.xpath("//*[text()='This is a sample page']")).getText();
	    	            	                System.out.println(data2+" no 2");
	    	            	                driver.close();

	    	            	                Thread.sleep(1000);
	    	            	            	String data1=   driver.switchTo().window(li.get(1)).findElement(By.xpath("//*[text()='This is a sample page']")).getText();
	    	            	            	                System.out.println(data1+"  no 1");
	    	            	            	                Thread.sleep(1000);
	    	            	            	                driver.close();
	        
	    	            	            	                
	        


	                Thread.sleep(1000);
	                driver.switchTo().window(li.get(0)); 
	               
	        }
	    }