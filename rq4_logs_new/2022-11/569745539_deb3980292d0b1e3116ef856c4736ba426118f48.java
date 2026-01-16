package pages;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.testng.Assert;

import baselibrary.BaseLibrary;
import excelutility.Excelutility1;

public class Webtable_TestingBABA_page extends BaseLibrary{
	public Webtable_TestingBABA_page() {
		PageFactory.initElements(driver,this);
	}

	@FindBy(xpath = "//*[@data-target=\"#elements\"]")
	private WebElement elements;
	@FindBy(xpath = "//*[text()='web tables']")
	private WebElement webtable;
	
	@FindBy(xpath = "//*[@src=\"Webtable.html\"]")
	private WebElement frame;
	
	@FindBy(xpath = "//*[@pattern=\"^[a-zA-Z][\\sa-zA-Z]{2,32}\"]")
	private WebElement name;
	@FindBy(xpath = "//*[@pattern=\"[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$\"]")
	private WebElement email;
	@FindBy(xpath = "//*[text()='Save']")
	private WebElement savebtn;
	
	
	public void clickonelements()
	{
		elements.click();
	}
	public void clickonwebtable()
	{
		webtable.click();
	}

	String path=System.getProperty("user.dir")+"/textdata/webtable.xlsx";
	public void filldetails() throws InterruptedException
	{
		driver.switchTo().frame(frame);
		for(int i=1;i<5;i++)
		{
		name.sendKeys(Excelutility1.getReaddata1(path, 0, i, 0));
		email.sendKeys(Excelutility1.getReaddata1(path, 0, i, 1));
		Thread.sleep(400);
		savebtn.click();
		}
		
    }
	@FindBy(xpath = "//*[text()='Edit']")
	private List<WebElement> edit;
	@FindBy(xpath = "//*[@name=\"edit_name\"]")
	private WebElement editname;
	@FindBy(xpath = "//*[@name=\"edit_email\"]")
	private WebElement editmail;
	@FindBy(xpath = "//*[text()='Update']")
	private WebElement update;
	
	public void editdata()
	{
		int i=1;
		for(WebElement we:edit)
		{
			we.click();
			editname.clear();
			editname.sendKeys(Excelutility1.getReaddata1(path, 0, i, 2));
			editmail.clear();
			editmail.sendKeys(Excelutility1.getReaddata1(path, 0, i, 3));
			update.click();
			i++;
		}	
	}
	

	public void getvadition()
	{
		ArrayList<String> actual=new ArrayList<String>();
		for(int i=1;i<actual.size()-1;i++)
		{
			actual.add(Excelutility1.getReaddata1(path, 0, i, 0));
		}
		
		ArrayList<String> expected=new ArrayList<String>();
		List<WebElement> liedit=driver.findElements(By.xpath("//*[@class=\"table table-bordered data-table\"]//following::tr/td[1]"));		
		for(int i=1;i<expected.size()-1;i++)
		{
			String data=liedit.get(i).getText();
			expected.add(data);
		}
		
		for(int i=1;i<actual.size()-1;i++)
		{
			Assert.assertEquals(expected.get(i), actual.get(i));
		}
	
	}		 
}