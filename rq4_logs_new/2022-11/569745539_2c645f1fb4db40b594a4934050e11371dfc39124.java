package pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.Select;
import baselibrary.BaseLibrary;
import excelutility.Excelutility1;


public class PracticeForm_Page extends BaseLibrary {

	
	public PracticeForm_Page() 
	{
		PageFactory.initElements(driver, this);
	}

	@FindBy(xpath = "//div[@class=\"col-md-3 col-sm-12\"]/following::input")
	private WebElement fillform;
	
	@FindBy(xpath = "//input[@id=\"firstName\"]")
	private WebElement name;
	
	@FindBy(xpath = "//input[@id=\"lastName\"]")
	private WebElement lastname;
	
	@FindBy(xpath = "//input[@id=\"userEmail\"]")
	private WebElement email;
	
	@FindBy(xpath = "//input[@id=\"gender-radio-1\"]")
	private WebElement radiobutton;
	
	@FindBy(xpath = "//input[@placeholder=\"Mobile Number\"]")
	private WebElement mobileno;
	
	@FindBy(xpath = "//input[@id=\"dateOfBirthInput\"]")
	private WebElement dob;
	
	@FindBy(xpath = "//div[@class=\"subjects-auto-complete__value-container subjects-auto-complete__value-container--is-multi css-1hwfws3\"]")
	private WebElement subject;
	
	@FindBy(xpath = "//input[@id=\"hobbies-checkbox-1\"]")
	private WebElement checkbox;
	
	@FindBy(xpath = "//input[@id=\"uploadPicture\"]")
	private WebElement caddress;

	@FindBy(xpath = "//div[text()='Select State']")
	private WebElement dropdown;
	
	@FindBy(xpath = "//div[@class=\"col-md-4 col-sm-12\"]//div[@id=\"city\"]//div[text()='Select City']")
	private WebElement city;
	
	String path="C:\\Users\\dell\\eclipse-workspace\\Practicees_HolePrograms\\textdata\\fill form.xlsx";
			//String path=System.getProperty("user.dir")+"textdata/fill form.xlsx";
	public void clickonoption()
	{
		
			fillform.click();
	}
	
	public void filldata()
	{
		for(int i=0;i<=10;i++)
		{
			name.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			lastname.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			email.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			radiobutton.click();
			mobileno.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			subject.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			caddress.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			
			
Select sel=new Select(dropdown);
sel.deselectByIndex(5);

			
			city.sendKeys(Excelutility1.getReaddata1(path, 0, 1, i));
			
			
			
			
			
		
		
		}
		
	}

}



