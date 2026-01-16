package pageobject;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import utility.WebDriverReusable;
import utility.ReadPropertyFile;

public class GooglePage{
	
	public WebDriver driver;
	Logger logger = Logger.getLogger("GooglePage");
	
	public GooglePage() {
		driver = WebDriverReusable.getDriver();
		PageFactory.initElements(driver,this);
	}
	
	@FindBy(xpath="//input[@name='q']")
	private WebElement searchDataBox;
	
	@FindBy(xpath = "//a[@href='https://www.jpmorgan.com/']")
	private WebElement jpmLink;
		
	@FindBy(xpath = "//div[@class='logo-desktop-only']//img[@class='first-logo']")
	private WebElement jpmLogo;
	
	/* --------Launching Application------------- */
	public void launchUrl() {
		String url = ReadPropertyFile.getConfigData("url");
		logger.info("Launching browser");
		driver.get(url);
		driver.manage().window().maximize();
		int wait = Integer.parseInt(ReadPropertyFile.getConfigData("implicitwait"));
		driver.manage().timeouts().implicitlyWait(wait, TimeUnit.SECONDS);
		int pageloadTime = Integer.parseInt(ReadPropertyFile.getConfigData("pageloadTimeout"));
		driver.manage().timeouts().pageLoadTimeout(pageloadTime, TimeUnit.SECONDS);
		
	}
	
	/* --------Getting Page Title------------- */
	public String getPageTitle() {
		logger.info("Verifying page title");
		String pageTitle = driver.getTitle();
		return pageTitle;
	}
	
	/* ---------Entering Data In Google Seach-------*/
	public void enterSearchData(String searchData ) {
		searchDataBox.sendKeys(searchData);
		searchDataBox.sendKeys(Keys.ENTER);
		logger.info("Search data entered successfully");
	}
	
	/* ---------Clicking link-------*/
	public void clickJPMLink() {
		jpmLink.click();
		int pageloadTime = Integer.parseInt(ReadPropertyFile.getConfigData("pageloadTimeout"));
		WebDriverWait wait = new WebDriverWait(driver,pageloadTime);
		wait.until(ExpectedConditions.visibilityOf(jpmLogo));
	}
	
	/* ---------Verifying JPM logo-------*/
	public boolean verifyJPMLogo() {
		logger.info("Verifying JPM logo");
		return jpmLogo.isDisplayed();
	}
	
}