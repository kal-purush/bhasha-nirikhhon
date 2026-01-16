package assignments;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import java.time.Duration;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;

public class Assignment2 {
	WebDriver driver;

  @Test
  public void Test1() {
	  WebElement tableElement=driver.findElement(By.id("table2"));
	  List <WebElement> rowElement= tableElement.findElements(By.tagName("tr"));
	  System.out.println("Read table and Print data");
	  for (WebElement row:rowElement)
	  {
		  List <WebElement> columnElement=row.findElements(By.tagName("td"));
		  for(WebElement column:columnElement)
		  {
			  String columnValue=column.getText();
			  System.out.print(columnValue+" |  ");
		  }
		  System.out.println();
	  }
	  
  }
  
  @Test
  public void Test2() {
	
	String[][] data= {{"Smith","John","jsmith@gmail.com","$50.00"},
					  {"Bach","Frank","fbach@yahoo.com","$51.00"},
					  {"Doe","Jason","jdoe@hotmail.com","$100.00"},
					  {"Conway","Tim","tconway@earthlink.net","$50.00"}
	
	};
	String [][]tlData=new String [4][4];
	
	WebElement tableElement=driver.findElement(By.id("table2"));
	List <WebElement> rowElement= tableElement.findElements(By.tagName("tr"));
	
	for(int i=1;i<rowElement.size();i++)
	{
		
		WebElement row=rowElement.get(i);
		List <WebElement> columnElement=row.findElements(By.tagName("td"));
		
		for(int j=0;j<4;j++)
		{
			WebElement column=columnElement.get(j);			
			tlData[i-1][j]=column.getText();
			//String attribueVal=column.getDomAttribute("class");
			String attribueVal=column.getAttribute("class");
			//System.out.println(attribueVal);
			System.out.println(attribueVal+" from array is: "+ data[i-1][j] +" || "+attribueVal +" from table is: "+tlData[i-1][j]);

			Assert.assertEquals(data[i-1][j], tlData[i-1][j]);
	}
		
	}
		  
  }
  

  @Test
  public void Test3() {
	  WebElement tableElement=driver.findElement(By.id("table2"));
	  List <WebElement> rowElement= tableElement.findElements(By.tagName("tr"));
	  WebElement row=rowElement.get(2);
	  List <WebElement> columnElement=row.findElements(By.tagName("td"));
	  WebElement linkElement=columnElement.get(4);
	  String link=linkElement.getText();
	 // System.out.println(link);
	  driver.get(link);
	  System.out.println("Title of the Webpage: "+driver.getTitle());
		  
	  
	  
  }
  
  
  
  @BeforeMethod
  public void beforeMethod() {
	  
	  driver = new ChromeDriver();
	  driver.get("https://the-internet.herokuapp.com/tables");
	  driver.manage().window().maximize();
	  driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(3));
  }

  @AfterMethod
  public void afterMethod() {
	  
	  driver.quit();
  }

}