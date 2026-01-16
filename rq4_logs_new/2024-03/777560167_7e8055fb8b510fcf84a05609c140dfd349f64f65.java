package org.easy2excel;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class Shadoww {
	public static void main(String[] args) throws InterruptedException  {
System.setProperty("webdriver.chrome.driver","./drivers/chromedriver.exe");
ChromeOptions ops = new ChromeOptions();

ops.addArguments("--remote-allow-origins=*");
	    WebDriver driver = new ChromeDriver(ops);
	    driver.manage().window().maximize();
	    driver.get("https://www.spencersonline.com/product/multi-pack-cz-curved-barbells-5-pack-16-gauge/166097.uts");
	    driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);

Thread.sleep(5000);
//             <--TO SWITCH TO SHADOW RROT-->
WebElement elee = driver.findElement(By.xpath("PASS_SHADOW_ELEMENTS_XPATH")).getShadowRoot().findElement(By.xpath("PASS_IFRAME_XPATH"));
//             <--TO CLICK ON ELEMENT INSIDE IFRAME-->
driver.switchTo().frame(elee).findElement(By.xpath("PASS_ELEMENT_XPATH")).click();

//             <--TO GET ATTRIBUTE-->
String TextFetched=driver.switchTo().frame(elee).findElement(By.xpath("PASS_ELEMENT_XPATH")).getAttribute("aria-label");
	
	
	}

}