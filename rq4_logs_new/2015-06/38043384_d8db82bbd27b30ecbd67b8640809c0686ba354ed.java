package Ali;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class Anjum 
{

	public static void main(String[] args) {
		  WebDriver x = new FirefoxDriver();
		  x.get("http://iub.com/");
		  WebElement y = x.findElement(By.xpath("//frameset/frame[@name='top']"));
		  System.out.println(y);
		  x.close();
		 }
}