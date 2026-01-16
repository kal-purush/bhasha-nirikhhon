package genericLib;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.support.PageFactory;
import org.testng.Reporter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.asserts.SoftAssert;

import objectRepoLib.Home;
import objectRepoLib.Login;

public class BaseClass {
	

	
	/*Global Objects*/
	public static WebDriver driver;
	public FileLib flib =new FileLib();
	
	@BeforeClass
	public void configBC() throws Throwable
	{	
		//2. load all the keys and values data in the properties obj
		Reporter.log("=========Browser launching=======",true);
		/*read browser name from properties File*/
		//FileLib flib=new FileLib();
		//String BROWSERNAME=flib.getPropertyFileData("browser");
		String BROWSERNAME=flib.getPropertyValue("browser");
		
		if(BROWSERNAME.equals("firefox")){
			driver=new FirefoxDriver();
		}else if(BROWSERNAME.equals("chrome")){
			driver=new ChromeDriver();
		}else if(BROWSERNAME.equals("ie")){
			driver=new InternetExplorerDriver();
		}else{
            driver = new FirefoxDriver();	
		  }
		
		
		
	}
	@BeforeMethod
	public void configBM() throws Throwable
	{
		
		 /*read URL , username , password  from properties File*/

		String URL=flib.getPropertyValue("url");
		String USERNAME=flib.getPropertyValue("username");
		String PASSWORD=flib.getPropertyValue("password");
		
		
		driver.manage().window().maximize();
		driver.get(URL);
		driver.manage().timeouts().implicitlyWait(20, TimeUnit.SECONDS);
		
		/*create an object LOGIN POM class*/
		
		//Login1 lp=PageFactory.initElements(driver, Login1.class);
		Login lp=PageFactory.initElements(driver, Login.class);
		//Login1 lp99=PageFactory.initElements(driver, Login1.class);
		lp.loginToApp(USERNAME, PASSWORD);	
		
		//3.read the data
				//driver=new FirefoxDriver();
				//driver.get(URL);
		
		Reporter.log("=======login=======",true);
		//driver.findElement(By.name("user_name")).sendKeys(USERNAME);
		//driver.findElement(By.name("user_password")).sendKeys(PASSWORD);
		//driver.findElement(By.id("submitButton")).click();
		driver.manage().timeouts().implicitlyWait(20, TimeUnit.SECONDS);
		
		
		String expHomePage="vtiger CRM 5 - Commercial Open Source CRM";
		String actHomePage=driver.getTitle();
		
		SoftAssert s=new SoftAssert();
		s.assertEquals(actHomePage, expHomePage);
		Reporter.log("login successfully", true);
		
		//read the data from excel
		

	}
	@AfterMethod
	public void configAM()
	{
		Home hp=PageFactory.initElements(driver, Home.class);
		//Home1 hp=PageFactory.initElements(driver, Home1.class);
		hp.getLogOutLnk();
		
		/*WebElement wb=driver.findElement(By.xpath("//img[@src='themes/softed/images/user.PNG']"));
		Actions act=new Actions(driver);
		act.moveToElement(wb);
		act.perform();
		driver.findElement(By.xpath("//a[(text()='Sign Out')]")).click();	*/
	}
	@AfterClass
	public void configAc()
	{
		String expogOutPage="vtiger CRM 5 - Commercial Open Source CRM";
		String actLogOutPage=driver.getTitle();
	    SoftAssert s=new SoftAssert();
	    s.assertEquals(actLogOutPage, expogOutPage);
	    
	    Reporter.log("logout successfully", true);
		
		
		System.out.println("======closing browser=======");
		driver.quit();
    }

}