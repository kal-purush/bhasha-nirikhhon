package Com.techcanvas.seleniumdemo;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.Select;
import java.util.concurrent.TimeUnit;

public class AddDoctor {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		WebDriver driver=new FirefoxDriver();
		driver.get("http://medopsplus.techcanvass.co.in");
		driver.manage().window().maximize();
		
		WebElement element=driver.findElement(By.id("txtUserName"));
		element.sendKeys("avinash");
		WebElement element1=driver.findElement(By.id("txtPassword"));
		element1.sendKeys("avinash");
		
		driver.findElement(By.id("LoginButton")).click();
		
		//Select dropdown=new Select(driver.findElement(By.className("NavigationMenu_1 NavigationMenu_3 NavigationMenu_10")));
		//dropdown.selectByIndex(7);
	//	.//td[@id="NavigationMenun2"]
		//WebElement element2 = driver.findElement(By.className("NavigationMenu_1 NavigationMenu_3 NavigationMenu_10"));
		 
     //   Actions action = new Actions(driver);
 
     //   action.moveToElement(element2).moveToElement(driver.findElement(By.linkText("Doctor Master"))).click().build();
		
		
		   Actions a1 = new Actions(driver);
		     a1.moveToElement(driver.findElement
		     (By.xpath(".//*[@id='NavigationMenun2']/table//tbody//tr//td/a"))).build().perform();
		     driver.manage().timeouts().implicitlyWait(60, TimeUnit.SECONDS);
		    
		    driver.findElement
		     (By.xpath(".//*[@id='NavigationMenun13']//td//table/tbody//tr//td/a")).click();
		   // xyz.click();
		  //   .//td[@id="NavigationMenun2"]//table//tbody//tr//td//a
		    
		    driver.findElement(By.id("ctl00_ContentPlaceHolder1_lbaddnew")).click();
		    
		  //  Select dropdown=new Select(driver.findElement(By.id("ctl00_ContentPlaceHolder1_DdlSpecilization")));
		   
		 //   dropdown.selectByVisibleText("ENT");
		    Select dropdown=new Select(driver.findElement(By.id("ctl00_ContentPlaceHolder1_DdlSpecilization")));
			   
		    dropdown.selectByVisibleText("ENT");
		    
		   
		    
		    WebElement ele_name=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtName"));
		    ele_name.sendKeys("Jayanta Pradhan");
		    
		    WebElement ele_dob=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtDOB"));
		    ele_dob.sendKeys("30-Jun-1992");
		    
		    
		    WebElement ele_add=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtAddress"));
		    ele_add.sendKeys("Mumbai");
		    
		    WebElement ele_pin=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtPin"));
		    ele_pin.sendKeys("400607");
		    
		    Select dropdown1=new Select(driver.findElement(By.id("ctl00_ContentPlaceHolder1_DDlmarital")));
			   
		    dropdown1.selectByVisibleText("Married");
		    
		    WebElement ele_qual=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtQualification"));
		    ele_qual.sendKeys("MBBS(USA)");
		    
		    WebElement ele_mob=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtMob_Number"));
		    ele_mob.sendKeys("9022709297");
		    
		    Select dropdown2=new Select(driver.findElement(By.id("ctl00_ContentPlaceHolder1_DdlCategory")));
			   
		    dropdown2.selectByVisibleText("Consultant");
		    
		    WebElement ele_email=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtEmail"));
		    ele_email.sendKeys("Jayant067@gmail.com");
		    
		    WebElement ele_doj=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtDOJ"));
		    ele_doj.sendKeys("01-Jan-2015");
		    
		    WebElement ele_city=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtCity"));
		    ele_city.sendKeys("Thane");
		    
		    Select dropdown3=new Select(driver.findElement(By.id("ctl00_ContentPlaceHolder1_DdlCountry")));
			   
		    dropdown3.selectByVisibleText("India");
		    
		    WebElement ele_anni=driver.findElement(By.id("ctl00_ContentPlaceHolder1_TxtAnniversary"));
		    ele_anni.sendKeys("Done");
		    
		    WebElement ele_save=driver.findElement(By.id("ctl00_ContentPlaceHolder1_BtnSave"));
		    ele_save.click();
		    
		    
		   
		    driver.close();
		
	}

}

/*
Server Error in '/' Application.

The string was not recognized as a valid DateTime. There is an unknown word starting at index 0.

Description: An unhandled exception occurred during the execution of the current web request. Please review the stack trace for more information about the error and where it originated in the code. 

Exception Details: System.FormatException: The string was not recognized as a valid DateTime. There is an unknown word starting at index 0.

Source Error: 

An unhandled exception was generated during the execution of the current web request. Information regarding the origin and location of the exception can be identified using the exception stack trace below.

Stack Trace: 


[FormatException: The string was not recognized as a valid DateTime. There is an unknown word starting at index 0.]
   System.DateTimeParse.Parse(String s, DateTimeFormatInfo dtfi, DateTimeStyles styles) +10973474
   System.Convert.ToDateTime(String value) +83
   BusinessProject.DoctorMaster.PassDateFormate(TextBox txt) in D:\BackUpForAllProject\GNS 17 mar by atul\GNS 9oct\HMS\BusinessProject\DoctorMaster.aspx.cs:344
   BusinessProject.DoctorMaster.BtnSave_Click(Object sender, ImageClickEventArgs e) in D:\BackUpForAllProject\GNS 17 mar by atul\GNS 9oct\HMS\BusinessProject\DoctorMaster.aspx.cs:353
   System.Web.UI.WebControls.ImageButton.OnClick(ImageClickEventArgs e) +115
   System.Web.UI.WebControls.ImageButton.RaisePostBackEvent(String eventArgument) +124
   System.Web.UI.WebControls.ImageButton.System.Web.UI.IPostBackEventHandler.RaisePostBackEvent(String eventArgument) +10
   System.Web.UI.Page.RaisePostBackEvent(IPostBackEventHandler sourceControl, String eventArgument) +13
   System.Web.UI.Page.RaisePostBackEvent(NameValueCollection postData) +35
   System.Web.UI.Page.ProcessRequestMain(Boolean includeStagesBeforeAsyncPoint, Boolean includeStagesAfterAsyncPoint) +1724

Version Information: Microsoft .NET Framework Version:4.0.30319; ASP.NET Version:4.0.30319.34274
*/