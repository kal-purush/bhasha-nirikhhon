package testng;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

public class practiceHtml {
	WebDriver driver;
	SoftAssert sa= new SoftAssert();
  @Test
  public void checkBox() throws InterruptedException

  {
	  driver =new ChromeDriver();
	  
	  driver.manage().window().maximize();
	  driver.get("file:///C:/Users/sandh/Downloads/H2o_Practice_Page.html");
	  Thread.sleep(5000);
	  List<WebElement> checkBox=driver.findElements(By.xpath("//input[@type='checkbox']"));
	  Assert.assertEquals(checkBox.size(),3);
	 // sa.assertAll();
	
  }
  @Test
  public void radioButton() throws InterruptedException
  {
	  Thread.sleep(5000);
	  List<WebElement> radioButton=driver.findElements(By.xpath("//input[@type='radio']"));
	  sa.assertEquals(radioButton.size(), 3);
	  sa.assertAll();
  }
  @Test
  public void dropDown() throws InterruptedException
  {
	  Thread.sleep(5000);
	  List<WebElement> dropDown =driver.findElements(By.xpath("//select[@id='dropdown-class-example']"));
	  sa.assertEquals(dropDown.size(), 1);
	  sa.assertAll();	  
  }
  @Test
  public void editBox() throws InterruptedException
  {
	  Thread.sleep(5000);
	  List<WebElement> editBox =driver.findElements(By.cssSelector("input.inputs"));
	  sa.assertEquals(editBox.size(), 3);
	  sa.assertAll();
	  
  }
  @Test
  public void hyperlinks() throws InterruptedException
  {
	  Thread.sleep(5000);
	  List<WebElement> hyperLinks =driver.findElements(By.xpath("//a"));
	  sa.assertEquals(hyperLinks.size(), 23);
	  sa.assertAll();
  }
  
}