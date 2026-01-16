package pages;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.Select;

import baselibrary.BaseLibrary;

public class DataProviderExam_1_page extends BaseLibrary{

	public DataProviderExam_1_page() 
	{
		PageFactory.initElements(driver, this);
	}
	
	
	@FindBy(xpath = "(//input[@id='name'])[2]")
	private WebElement username;
	
	@FindBy(xpath = "(//input[@name='email'])[3]")
	private WebElement useremail;
	
	@FindBy(xpath = "(//input[@id='mobile'])[2]")
	private WebElement mobile;
	
	@FindBy(xpath = "//select[@id='usercourse']")
	private WebElement chooseservice;
	
	@FindBy(xpath = "//textarea[@id='usermsg']")
	private WebElement usermsg;
	
	@FindBy(xpath = "//button[text()='Submit']")
	private WebElement submit;
	
//	public void filldetails()
//	{
//		registernow.click();
//	
//		
//	}
	public void fillname(String name)
	{
		username.sendKeys(name);
		
	}
	public void fillemail(String email)
	{
		useremail.sendKeys(email);;
		
	}
	
	public void fillphone()
	{
		mobile.sendKeys("9557611555");
		
	}
	
	public void dropdownselect()
	{
		Select sel=new Select(chooseservice);
		sel.selectByIndex(2);
	}
	public void fillmsg(String msg
			)
	{
		usermsg.sendKeys(msg);
		
	}
	public void submitbtn()
	{
		submit.click();
		
	}
}
