package main;


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
//import org.junit.*;
//import static org.junit.Assert.*;
import org.openqa.selenium.Alert;
import org.openqa.selenium.NoAlertPresentException;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.By;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.WebDriver;

public class Main {
  String username, password, siteid, eefname, eelname, employeeinit, employeepass, employeepassnew, employeeinitnew, url2, url1;
  private WebDriver driver;
  private boolean acceptNextAlert = true;
  private StringBuffer verificationErrors = new StringBuffer();
  static Logger log = Logger.getLogger(Main.class);
     
  
  long timeSeed = System.nanoTime(); // to get the current date time value
  double randSeed = Math.random() * 1000; // random number generation
  long midSeed = (long) (timeSeed * randSeed);  // mixing up the time and rand number variable timeSeed will be unique.

  String s = midSeed + "";
  String subStr = s.substring(0, 9);

  int myRandomNum = Integer.parseInt(subStr);    // integer value


  @BeforeSuite (alwaysRun=true)
  @Parameters({ "url","username","password","site","employeefn","employeeln","employeeinitials","employeepassword","employeepasswordnew","gohome" })
  public void setUp(String url, String user, String pwd, String ste, String eefn, String eeln, String eeinits, String eepw, String eepwnew, String gohome) throws Exception {
    username=user;
	password=pwd;
	siteid=ste;
	eefname=eefn;
	eelname=eeln;
	url2=(gohome+ste);
	url1=url;
	    
	employeeinit=eeinits;
	employeeinitnew=(eeinits+myRandomNum);
		
	employeepass=eepw;
	employeepassnew=eepwnew;
	
			
	driver = new FirefoxDriver();
    driver.get(url);
    driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
	


  }

	/////////
	//HOME///
	/////////
  
	@Test
	public void testNavigateHome() throws Exception {
	
	driver.get(url2);	
	   
	}
    
    
	///////////
	//LOGOUT///
	///////////
	@Test
	public void testLogOut() throws Exception {
	
	driver.findElement(By.linkText("Log Out")).click();
	Thread.sleep(2000);
	log.info("Logout successful");
	
	}
  
  
  /////////
  //LOGIN//
  /////////
  
  @Test
  public void testLogin() throws Exception {
	
	 
	//Maximize window
	driver.manage().window().maximize();
    
	//Login
	driver.findElement(By.id("TBSiteID")).clear();
    driver.findElement(By.id("TBSiteID")).sendKeys(siteid);
	driver.findElement(By.id("TBUserName")).clear();
    driver.findElement(By.id("TBUserName")).sendKeys(username);
    driver.findElement(By.name("TBPassword")).clear();
    driver.findElement(By.name("TBPassword")).sendKeys(password);
    Thread.sleep(500);
    driver.findElement(By.name("Login")).click();
    Thread.sleep(2000);
    log.info("LOGIN SUCCESSFUL");
    
  }
  
  
  //////////
  //ADD EE//
  //////////
  
  @Test
  public void testAddNormalEE() throws Exception {
	 
	//Click the search field to de-select drop down
    driver.findElement(By.id("QRY")).click();
    driver.findElement(By.id("QRY")).clear();
    
    //Click on Add Employee and fill in form
    driver.findElement(By.linkText("Add Employee")).click();
    Thread.sleep(1000);
    driver.findElement(By.name("TBNEWSSN")).clear();
    driver.findElement(By.name("TBNEWSSN")).sendKeys(""+myRandomNum);
    System.out.println("");
    System.out.println("New EE SSN: "+myRandomNum);
    System.out.println("");
        	
    driver.findElement(By.name("TBLname")).clear();
    driver.findElement(By.name("TBLname")).sendKeys(eelname);
    driver.findElement(By.name("TBFname")).clear();
    driver.findElement(By.name("TBFname")).sendKeys(eefname);
    driver.findElement(By.name("TBBdate")).clear();
    driver.findElement(By.name("TBBdate")).sendKeys("11/12/1970");
    new Select(driver.findElement(By.name("cmbGender"))).selectByVisibleText("Male");
    driver.findElement(By.name("TBID")).clear();
    driver.findElement(By.name("TBID")).sendKeys(""+myRandomNum);
    driver.findElement(By.name("tbUserName")).clear();
    driver.findElement(By.name("tbUserName")).sendKeys(employeeinitnew);
    driver.findElement(By.name("tbPwd")).clear();
    driver.findElement(By.name("tbPwd")).sendKeys(employeepass);
    
    //Confirm password?
    try{
    WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement pwconfirm1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("tbConfirm")));
	
	System.out.println("Confirm field found. Clicking it now.");	
	driver.findElement(By.name("tbConfirm")).sendKeys(employeepass);
	}
    catch(Throwable e){
	System.err.println("Confirm field was not found within the time.");    	   }
    
   
    
    //Area if needed//
    try{
    WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement tbExportid1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("TbExportID")));
	
	System.out.println("TbExportID found. Clicking it now.");	
	driver.findElement(By.name("TbExportID")).sendKeys("x");
	}
    catch(Throwable e){
	System.err.println("TbExportID was not found within the time.");    	   }
    
    driver.findElement(By.name("TBHRdate")).clear();
    driver.findElement(By.name("TBHRdate")).sendKeys("1/10/2015");
    
    
    //Address and salary
    driver.findElement(By.name("TBADDR1")).sendKeys("12345 Santa Ln");
    driver.findElement(By.name("TBPostal")).sendKeys("94523");
    driver.findElement(By.name("TBCity")).sendKeys("New York");
    new Select(driver.findElement(By.name("cmbState"))).selectByVisibleText("NY");
    
	
	//Country if needed
	
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement Country = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("cmbCountry")));
	
	System.out.println("Country Drop Down found. Clicking it now.");	
	driver.findElement(By.name("cmbCountry")).sendKeys("United States");
	}
    catch(Throwable e){
	System.err.println("Country Drop Down was not found within the time.");
	   }
  
    driver.findElement(By.name("TBPhone")).sendKeys("8185752589");
    
    
    
    //Area if needed//
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement Area = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("cmbArea")));
	
	System.out.println("Area Drop Down found. Clicking it now.");	
	new Select(driver.findElement(By.name("cmbArea"))).selectByIndex(2);
	}
    catch(Throwable e){
	System.err.println("Area drop down was not found within the time.");    	   }
  
    
    
    new Select(driver.findElement(By.name("CmbStatus"))).selectByVisibleText("Full Time Employee");
    new Select(driver.findElement(By.name("cmbPayCycle"))).selectByVisibleText("Bi-Weekly");
    driver.findElement(By.name("TBAnnualSalary")).clear();
    driver.findElement(By.name("TBAnnualSalary")).sendKeys("85000");
    driver.findElement(By.id("btUpdate_bottom")).click();
    Thread.sleep(3000);
    
    
        
    //CHECK FOR CORE CHECK BOXES
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement CoreLifeADD = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("CKLIFE_ADD")));
	
	System.out.println("Core Life ADD check box found. Clicking it now.");	
	driver.findElement(By.name("CKLIFE_ADD")).click();
	}
    catch(Throwable e){
	System.err.println("Core Life ADD check box was not found.");    	   }
    
    
    //CHECK FOR CORE CHECK BOXES
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement LongTerm = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("CKLTD")));
	
	System.out.println("Long Term Disability check box found. Clicking it now.");	
	driver.findElement(By.name("CKLTD")).click();
	}
    catch(Throwable e){
	System.err.println("Long Term Disability check box was not found.");    	   }
    
    
    
    //CHECK FOR CORE CHECK BOXES
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement CKEAP = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("CKEAP")));
	
	System.out.println("EE Assistance Program check box found. Clicking it now.");	
	driver.findElement(By.name("CKEAP")).click();
	}
    catch(Throwable e){
	System.err.println("EE Assistance Program check box was not found.");    	   }
    
    
    
    //CHECK FOR CORE CHECK BOXES
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement CoreLTD = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("CKCORELTD")));
	
	System.out.println("Core LTD check box found. Clicking it now.");	
	driver.findElement(By.name("CKCORELTD")).click();
	}
    catch(Throwable e){
	System.err.println("Core LTD check box was not found.");    	   }
    
    
    //CHECK FOR CORE CHECK BOXES
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement CoreSTD = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("CKCORESTD")));
	
	System.out.println("Core STD check box found. Clicking it now.");	
	driver.findElement(By.name("CKCORESTD")).click();
	}
    catch(Throwable e){
	System.err.println("Core STD check box was not found.");    	   }
    
          
	//Update if needed//
	
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement Update = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("cmdUpdate")));
	
	System.out.println("Update button found. Clicking it now.");	
	driver.findElement(By.name("cmdUpdate")).click();
	}
    catch(Throwable e){
	System.err.println("Update button was not found within the time.");
    }
	Thread.sleep(2000);
    
	
    //Click ok on All Core Benefits are now enrolled
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ok = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("cmdUpdate")));
	
	System.out.println("OK button found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
    catch(Throwable e){
	System.err.println("OK button was not found within the time.");  
    }
    Thread.sleep(2000);
       
    log.info("ADD EE SUCCESSFUL");
    
  }
  
	/////////////////////
	////TERMINATE EE/////
	/////////////////////
	
	@Test
	public void testTerminateEE() throws Exception {
	
	//Click the search field to de-select drop down
	driver.findElement(By.name("QRY")).click();
	driver.findElement(By.name("QRY")).sendKeys(eelname);
	driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	Thread.sleep(3000);
	
	//Find Employee to Terminate
	driver.findElement(By.linkText(eefname)).click();
	Thread.sleep(2000);
	driver.findElement(By.linkText(eefname+" "+eelname)).click();
	Thread.sleep(2000);
	new Select(driver.findElement(By.name("CmbStatus"))).selectByVisibleText("Terminated");
	driver.findElement(By.name("tbTermDate")).clear();
	driver.findElement(By.name("tbTermDate")).sendKeys("11/10/2014");
	driver.findElement(By.id("btUpdate_bottom")).click();
	Thread.sleep(2000);
	
	try{
	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement lookforReason = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("REASONCD")));
	
	System.out.println("Reason drop down found. Clicking it now.");	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	}
	catch(Throwable e){
	System.err.println("Reason drop down was not found within the time.");
	}
	
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement lookforTermbtn = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("cmdTerminate")));
	
	System.out.println("Terminate button found. Clicking it now.");	
	driver.findElement(By.name("cmdTerminate")).click();
	}
	catch(Throwable e){
	System.err.println("Terminate button was not found within the time.");
	}
	
	driver.findElement(By.name("btn")).click();
	Thread.sleep(1000);
	log.info("Terminate EE successful");
	}
	 
  
	/////////////////
	//ADD DEPENDENT//
	/////////////////
	
	 long timeSeed2 = System.nanoTime(); // to get the current date time value
	  double randSeed2 = Math.random() * 1000; // random number generation
	  long midSeed2 = (long) (timeSeed2 * randSeed2);  // mixing up the time and rand number variable timeSeed will be unique.

	  String s2 = midSeed2 + "";
	  String subStr2 = s2.substring(0, 9);

	  int myRandomNum2 = Integer.parseInt(subStr2);    // integer value

	@Test
	public void testAddDependent() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
  
    //Add New Dependent
  driver.findElement(By.linkText("Add Dependent")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("TBNEWSSN")).sendKeys(""+myRandomNum2);
  driver.findElement(By.name("TBFname")).sendKeys("Connor");
  driver.findElement(By.name("TBLname")).sendKeys("Bird");
  driver.findElement(By.name("TBBdate")).sendKeys("11/10/2002");
  new Select(driver.findElement(By.name("cmbGender"))).selectByIndex(1);
  new Select(driver.findElement(By.name("CmbStatus"))).selectByIndex(1);
  driver.findElement(By.name("btUpdate")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("btn")).click();
  Thread.sleep(3000);
  log.info("ADD DEPENDENT SUCCESSFUL");

}

	
////////////////////
//DELETE DEPENDENT//
////////////////////

@Test
public void testDeleteDependent() throws Exception {

	
	
}



/////////////////
//ADD MEDICAL////
/////////////////

	@Test
	public void testAddMedical() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
 
  Thread.sleep(2000);
  
       
  //Medical Benefits
  driver.findElement(By.xpath("//form[@id='submitform112293823']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(3000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  
  //Pick Plan
  try{
   	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("chkSjwiD20131024133247")));
	
	System.out.println("Checkbox1 found. Clicking it now.");	
	driver.findElement(By.id("chkSjwiD20131024133247")).click();
	}
   catch(Throwable e){
	System.err.println("Checkbox1 was not found within the time.");
	}
  
  
  try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox2 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("chkMaQrf20101101103803")));
	
	System.out.println("Checkbox2 found. Clicking it now.");	
	driver.findElement(By.id("chkMaQrf20101101103803")).click();
	}
   catch(Throwable e){
	System.err.println("Checkbox2 was not found within the time.");
	}
     
  //Continue button
  try{
   	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnbutton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
   catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
  Thread.sleep(2000);
  
  
  //I agree button
  try{
   	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnbutton001 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
   catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
  Thread.sleep(2000);
  log.info("ADD MEDICAL SUCCESSFUL");
  
}


/////////////////
//ADD DENTAL/////
/////////////////

@Test
public void testAddDental() throws Exception {
//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
  
  Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,350)", "");
    
  //Dental Benefits
  driver.findElement(By.xpath("//form[@id='submitform110009356']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  
  //Check option
  try{
  	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(".//*[@id='chkoGG2W20100920135344']")));
	
	System.out.println("Check box found. Clicking it now.");	
	driver.findElement(By.xpath(".//*[@id='chkoGG2W20100920135344']")).click();
	}
   catch(Throwable e){
	System.err.println("Check box was not found within the time.");
  }
  Thread.sleep(1000);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(3000);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(3000);
  log.info("Add Dental successful");

}


/////////////////
///ADD VISION////
/////////////////

@Test
public void testAddVision() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
    
  Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,450)", "");
    
  //Vision Benefits
  driver.findElement(By.xpath("//form[@id='submitform110009041']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(3000);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(3000);
  log.info("Add Vision successful");

}


/////////////////////////
///ADD CORE LIFE/AD&D////
/////////////////////////

@Test
public void testAddCoreLife() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
  
  Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,600)", "");
    
  //Core Life ADD Benefits
  driver.findElement(By.xpath("//form[@id='submitform110038560']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  
  //Pick Plan
   try{
    	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkgbqyu20131104135446")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkgbqyu20131104135446")).click();
	}
    catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	   }
  
  
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(3000);
  
  //Primary Beneficiaries
  driver.findElement(By.name("NameP1")).clear();
  driver.findElement(By.name("NameP1")).sendKeys("John Bryant");
  driver.findElement(By.name("RelationP1")).clear();
  driver.findElement(By.name("RelationP1")).sendKeys("Son");
  driver.findElement(By.name("PercentP1")).clear();
  driver.findElement(By.name("PercentP1")).sendKeys("50");
  driver.findElement(By.name("NameP2")).clear();
  driver.findElement(By.name("NameP2")).sendKeys("Kelly Bryant");
  driver.findElement(By.name("RelationP2")).clear();
  driver.findElement(By.name("RelationP2")).sendKeys("Wife");
  driver.findElement(By.name("PercentP2")).clear();
  driver.findElement(By.name("PercentP2")).sendKeys("50");
  
  //Contingent Beneficiaries
  driver.findElement(By.name("NameC1")).clear();
  driver.findElement(By.name("NameC1")).sendKeys("John Wayne");
  driver.findElement(By.name("RelationC1")).clear();
  driver.findElement(By.name("RelationC1")).sendKeys("Brother");
  driver.findElement(By.name("PercentC1")).clear();
  driver.findElement(By.name("PercentC1")).sendKeys("50");
  driver.findElement(By.name("NameC2")).clear();
  driver.findElement(By.name("NameC2")).sendKeys("Sally Sue");
  driver.findElement(By.name("RelationC2")).clear();
  driver.findElement(By.name("RelationC2")).sendKeys("Sister");
  driver.findElement(By.name("PercentC2")).clear();
  driver.findElement(By.name("PercentC2")).sendKeys("50");
  Thread.sleep(2000);
  driver.findElement(By.name("SUBMIT")).click();
  
  Thread.sleep(2000);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(4000);
  log.info("ADD CORE LIFE AD&D SUCCESSFUL");

}


	//////////////////////////////
	///ADD CORE DEPENDENT LIFE////
	//////////////////////////////
	
	@Test
	public void testAddCoreDependentLife() throws Exception {
	//Search for employee
	driver.findElement(By.name("QRY")).click();
	driver.findElement(By.name("QRY")).clear();
	driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
	driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	  
	Thread.sleep(2000);
	  
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,750)", "");
	
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform116163354']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("O")).click();
	driver.findElement(By.name("O")).clear();
	driver.findElement(By.name("O")).sendKeys("11/10/2014");
	driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
	Thread.sleep(3000);
	
	//PICK PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkgbqyu20131104135446")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkgbqyu20131104135446")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
	
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	
	
	//CHECK IF THERE IS A REASON DROP DOWN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkforreasoncd = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("REASONCD")));
	
	System.out.println("Reason drop down found. Clicking it now.");	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	}
	catch(Throwable e){
	System.err.println("Reason drop down was not found within the time.");
	}
	
	//I AGREE
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	log.info("ADD CORE DEPENDENT LIFE SUCCESSFUL");
	
	}
	
	
	
	//////////////////////////////
	///ADD LONG TERM DISABILITY///
	//////////////////////////////
	
	@Test
	public void testAddLongTermDisability() throws Exception {
	//Search for employee
	driver.findElement(By.name("QRY")).click();
	driver.findElement(By.name("QRY")).clear();
	driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
	driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	  
	Thread.sleep(2000);
	  
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,980)", "");
	
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform110030792']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("O")).click();
	driver.findElement(By.name("O")).clear();
	driver.findElement(By.name("O")).sendKeys("11/10/2014");
	driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
	Thread.sleep(3000);
	
	//PICK PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkgbqyu20131104135446")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkgbqyu20131104135446")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
	
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	
	
	//CHECK IF THERE IS A REASON DROP DOWN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkforreasoncd = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("REASONCD")));
	
	System.out.println("Reason drop down found. Clicking it now.");	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	}
	catch(Throwable e){
	System.err.println("Reason drop down was not found within the time.");
	}
	
	//I AGREE
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	log.info("ADD LONG TERM DISABILITY SUCCESSFUL");
	
	}
	
	///////////////////////////////////
	//////////ADD VOLUNTARY STD////////
	///////////////////////////////////
	
	@Test
	public void testAddVoluntarySTD() throws Exception {
	//Search for employee
    driver.findElement(By.name("QRY")).click();
    driver.findElement(By.name("QRY")).clear();
    driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
    driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
    
    Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,450)", "");
    
  //Core Life ADD Benefits
  driver.findElement(By.xpath("//form[@id='submitform111295104']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  
  //Pick Plan
  try{
   	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkTradd20131104134004")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkTradd20131104134004")).click();
	}
   catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	   }
  
  //driver.findElement(By.id("chkTradd20131104134004")).click();
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(2000);
  
  new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(5000);
  log.info("ADD VOLUNTARY STD SUCCESSFUL");

}

	//////////////////////////////
	////ADD VOLUNTARY EE LIFE/////
	//////////////////////////////
	
	@Test
	public void testAddVoluntaryEELife() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	  
  Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,1150)", "");
    
  //HealthCare FSA Benefits
  driver.findElement(By.xpath("//form[@id='submitform110450913']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(2000);
  new Select(driver.findElement(By.name("WJmNQ20131104140548VOLUME"))).selectByIndex(2);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(2000);
  
  //Primary Beneficiaries
  driver.findElement(By.name("NameP1")).clear();
  driver.findElement(By.name("NameP1")).sendKeys("Suzie Calvin");
  driver.findElement(By.name("RelationP1")).clear();
  driver.findElement(By.name("RelationP1")).sendKeys("Daughter");
  driver.findElement(By.name("PercentP1")).clear();
  driver.findElement(By.name("PercentP1")).sendKeys("50");
  driver.findElement(By.name("NameP2")).clear();
  driver.findElement(By.name("NameP2")).sendKeys("Kelly Crile");
  driver.findElement(By.name("RelationP2")).clear();
  driver.findElement(By.name("RelationP2")).sendKeys("Daughter");
  driver.findElement(By.name("PercentP2")).clear();
  driver.findElement(By.name("PercentP2")).sendKeys("50");
  
  //Contingent Beneficiaries
  driver.findElement(By.name("NameC1")).clear();
  driver.findElement(By.name("NameC1")).sendKeys("John Wayne");
  driver.findElement(By.name("RelationC1")).clear();
  driver.findElement(By.name("RelationC1")).sendKeys("Brother");
  driver.findElement(By.name("PercentC1")).clear();
  driver.findElement(By.name("PercentC1")).sendKeys("50");
  driver.findElement(By.name("NameC2")).clear();
  driver.findElement(By.name("NameC2")).sendKeys("Sally Sue");
  driver.findElement(By.name("RelationC2")).clear();
  driver.findElement(By.name("RelationC2")).sendKeys("Sister");
  driver.findElement(By.name("PercentC2")).clear();
  driver.findElement(By.name("PercentC2")).sendKeys("50");
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(2000);
  
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(3000);
  log.info("ADD VOLUNTARY EE LIFE successful");

}
	
	//////////////////////////////
	//////////ADD CORE STD////////
	//////////////////////////////
	
	@Test
	public void testAddCoreSTD() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	  
  Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,450)", "");
    
  //Core Life ADD Benefits
  driver.findElement(By.xpath("//form[@id='submitform115029826']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  
  //Pick Plan
  try{
   	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkTradd20131104134004")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkTradd20131104134004")).click();
	}
   catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	   }
  
  //driver.findElement(By.id("chkTradd20131104134004")).click();
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(2000);
  
  new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(5000);
  log.info("ADD CORE STD SUCCESSFUL");

}


	////////////////////////////
	////////ADD CORE LTD////////
	////////////////////////////
	
	@Test
	public void testAddCoreLTD() throws Exception {
	//Search for employee
  driver.findElement(By.name("QRY")).click();
  driver.findElement(By.name("QRY")).clear();
  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	  
  Thread.sleep(2000);
  
  //Scroll down for second Manage Benefits or lower  
  JavascriptExecutor jse = (JavascriptExecutor)driver;
  jse.executeScript("window.scrollBy(0,950)", "");
    
  //Core Life ADD Benefits
  driver.findElement(By.xpath("//form[@id='submitform112600696']//ul[@class='menuUL opMenu']")).click();
  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
  Thread.sleep(2000);
    
  
  driver.findElement(By.name("O")).click();
  driver.findElement(By.name("O")).clear();
  driver.findElement(By.name("O")).sendKeys("11/10/2014");
  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
  Thread.sleep(3000);
  
//Pick Plan
  try{
   	
   	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
   	WebDriverWait wait = new WebDriverWait(driver,5);
   	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkNCn9W20131104134627")));
   	
   	System.out.println("Checkbox found. Clicking it now.");	
   	driver.findElement(By.name("chkNCn9W20131104134627")).click();
   	}
       catch(Throwable e){
   	System.err.println("Checkbox was not found within the time.");
   	   }
  
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(2000);
  
  new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
  driver.findElement(By.name("SUBMIT")).click();
  Thread.sleep(5000);
  log.info("ADD CORE LTD SUCCESSFUL");

}



	////////////////////////////
	////ADD HEALTH CARE FSA/////
	////////////////////////////
	
	@Test
	public void testAddHealthCareFSA() throws Exception {
	  //Search for employee
	  driver.findElement(By.name("QRY")).click();
	  driver.findElement(By.name("QRY")).clear();
	  driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
	  driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	  
	  Thread.sleep(2000);
  
	  //Scroll down for second Manage Benefits or lower  
	  JavascriptExecutor jse = (JavascriptExecutor)driver;
	  jse.executeScript("window.scrollBy(0,850)", "");
	    
	  //HealthCare FSA Benefits
	  driver.findElement(By.xpath("//form[@id='submitform112293795']//ul[@class='menuUL opMenu']")).click();
	  driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
	  Thread.sleep(2000);
	  driver.findElement(By.name("O")).click();
	  driver.findElement(By.name("O")).clear();
	  driver.findElement(By.name("O")).sendKeys("11/10/2014");
	  driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
	  Thread.sleep(2000);
	  driver.findElement(By.name("02pzv20131120115631VOLUME")).sendKeys("2500");
	  driver.findElement(By.name("SUBMIT")).click();
	  Thread.sleep(2000);
	  driver.findElement(By.name("SUBMIT")).click();
	  Thread.sleep(2000);
	  log.info("ADD HEALTH CARE FSA SUCCESSFUL");
	
	}


	///////////////////////////////
	////ADD DEPENDENT CARE FSA/////
	///////////////////////////////
	
	@Test
	public void testAddDependentCareFSA() throws Exception {
	//Search for employee
	driver.findElement(By.name("QRY")).click();
	driver.findElement(By.name("QRY")).clear();
	driver.findElement(By.name("QRY")).sendKeys(""+myRandomNum);
	driver.findElement(By.name("QRY")).sendKeys(Keys.ENTER);
	Thread.sleep(2000);
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,1050)", "");
	  
	//HealthCare FSA Benefits
	driver.findElement(By.xpath("//form[@id='submitform111735757']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add Coverage: (Prompt Effective)")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("O")).click();
	driver.findElement(By.name("O")).clear();
	driver.findElement(By.name("O")).sendKeys("11/10/2014");
	driver.findElement(By.name("O")).sendKeys(Keys.ENTER);
	Thread.sleep(2000);
	driver.findElement(By.name("P6w97T20101101135938VOLUME")).sendKeys("2500");
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	log.info("ADD DEPENDENT CARE FSA SUCCESSFUL");

}




	/////////////////////////////////////////////////
	////BATCH PROCESSING ENROLL ALL MISSING CORE/////
	/////////////////////////////////////////////////
	@Test
	public void testBatchProcessingEnrollAllMissingCore() throws Exception {
	driver.findElement(By.linkText("Batch Processing")).click();
	Thread.sleep(2000);
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,550)", "");
	Thread.sleep(3000);
	    
	driver.findElement(By.xpath(".//*[@id='form1']/input[5]")).click();
	Thread.sleep(3000);
	
	driver.findElement(By.name("btn")).click();
	Thread.sleep(3000);
	log.info("BATCH PROCESSING ENROLL ALL MISSING CORE STARTED");
}


	////////////////////////////////////
	////BATCH PROCESSING RATE TABLE/////
	////////////////////////////////////
	@Test
	public void testBatchProcessingRateTable() throws Exception {
  driver.findElement(By.linkText("Batch Processing")).click();
  //driver.findElement(By.name("FileName")).clear();
  driver.findElement(By.name("FileName")).sendKeys("H:\\QA\\Selenium Import Files\\PACK2552_RateTable.xls");
  new Select(driver.findElement(By.name("SPONSOR"))).selectByVisibleText("RateTable");
  driver.findElement(By.name("cmdSend")).click();
  
  for (int second = 0;; second++) {
  	if (second >= 60) Assert.fail("timeout");
  	try { if ("Message".equals(driver.getTitle())) break; } catch (Exception e) {}
  	Thread.sleep(1000);
  }

  driver.findElement(By.name("btn")).click();
  Thread.sleep(2000);
  
  log.info("BATCH PROCESSING RATE TABLE STARTED");
}



	///////////////////////////////////////////////////////////////////
	////BATCH PROCESSING CHECK ELIGIBILITY ON EXISTING ENROLLMENTS/////
	///////////////////////////////////////////////////////////////////
	
	@Test
	public void testBatchProcessingCheckEligibilityonExistingEnrollments() throws Exception {
	driver.findElement(By.linkText("Batch Processing")).click();
	Thread.sleep(2000);
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,550)", "");
	Thread.sleep(3000);
	
	driver.findElement(By.xpath(".//*[@id='form1']/input[5]")).click();
	Thread.sleep(3000);
	
	driver.findElement(By.name("btn")).click();
	Thread.sleep(2000);
	log.info("BATCH PROCESSING CHECK ELIGIBILITY ON EXISTING ENROLLMENTS STARTED");
	}
	
	
	


	/////////////////////
	////AUDIT REPORT/////
	/////////////////////
	@Test
	public void testAuditReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[5]/a"));
  builder.moveToElement(tools).perform();
  builder.moveToElement(cobra).click().perform();
  Thread.sleep(3000);
  
	
  //Click on Audit Report
  driver.findElement(By.linkText("Audit Report")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("fldunder15")).click();
  driver.findElement(By.id("fldover64")).click();
  driver.findElement(By.id("fldstillactive")).click();
  driver.findElement(By.id("fldexpiredcobra")).click();
  driver.findElement(By.id("MISSINGENR_ 2")).click();
 
  String oldTab = driver.getWindowHandle();
  driver.findElement(By.name("Run")).click();
  log.info("AUDIT REPORT STARTED");
  
  Thread.sleep(25000);
  // considering that there is only one tab opened in that point.
  ArrayList<String> newTab = new ArrayList<String>(driver.getWindowHandles());
  newTab.remove(oldTab);
  // change focus to new tab
  driver.switchTo().window(newTab.get(0));
  // Do what you want here, you are in the new tab
  
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
		
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click(); }
	  catch(Throwable e){
	  System.err.println("Button not found within the time.");  
	  }
  
  driver.close();
  // change focus back to old tab
  driver.switchTo().window(oldTab);
  Thread.sleep(5000);
   
}
	
	/////////////////////////////
	////BENEFICIARIES REPORT/////
	/////////////////////////////
	@Test
	public void testBeneficiariesReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[5]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	
  ///Click on Beneficiaries Report
  String oldTab = driver.getWindowHandle();
  driver.findElement(By.linkText("Beneficiaries")).click();
  log.info("BENEFICIARIES REPORT STARTED");
  Thread.sleep(15000);
   
  // considering that there is only one tab opened in that point.
  ArrayList<String> newTab = new ArrayList<String>(driver.getWindowHandles());
  newTab.remove(oldTab);
  // change focus to new tab
  driver.switchTo().window(newTab.get(0));
  // Do what you want here, you are in the new tab
  
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
		
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab);
	   }
   
  Thread.sleep(5000);

   
}
	
	/////////////////////////////
	////BILLING MATRIX REPORT/////
	/////////////////////////////
	@Test
	public void testBillingMatrixReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	
 
  //Click on Billing Matrix
  driver.findElement(By.linkText("Billing Matrix")).click();
  Thread.sleep(2000);
	driver.findElement(By.name("FROMDATE")).clear();
  driver.findElement(By.name("FROMDATE")).sendKeys("09/01/2014");
  driver.findElement(By.id("C1_1")).click();
  driver.findElement(By.id("C13_1")).click();
  driver.findElement(By.id("C11_1")).click();
  driver.findElement(By.id("fldstatus")).click();
  driver.findElement(By.id("fldage")).click();
  driver.findElement(By.id("fldsmoker")).click();
  String oldTab2 = driver.getWindowHandle();
  driver.findElement(By.xpath("(//input[@name='Run'])[2]")).click();
  log.info("BILLING MATRIX REPORT STARTED");
  Thread.sleep(15000);
 
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab2);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab2);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab2);
	   }
      Thread.sleep(5000);
   
}	

	///////////////////////
	////BILLING REPORT/////
	///////////////////////
	@Test
	public void testBillingReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	
  //Click on Billing Report
  driver.findElement(By.linkText("Billing")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("FROMDATE")).clear();
  driver.findElement(By.name("FROMDATE")).sendKeys("09/01/2014");
  driver.findElement(By.id("C1_1")).click();
  driver.findElement(By.id("C2_1")).click();
  driver.findElement(By.id("D1_1")).click();
  String oldTab2 = driver.getWindowHandle();
  driver.findElement(By.xpath("(//input[@name='Run'])[2]")).click();
  Thread.sleep(15000);
 
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab2);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab2);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab2);
	   }
      Thread.sleep(5000);

   
}		
	
	/////////////////////////////
	////COST BENEFIT REPORT/////
	/////////////////////////////
	@Test
	public void testCostBenefitReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	
  //Click on Benefit Cost Report
  driver.findElement(By.linkText("Benefit Cost")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("C3_1")).click();
  driver.findElement(By.id("C8_1")).click();
  driver.findElement(By.id("fldhdate")).click();
  String oldTab3 = driver.getWindowHandle();
  driver.findElement(By.xpath("(//input[@name='Run'])[2]")).click();
  Thread.sleep(25000);
         
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab3);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab3);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab3);
	   }
      Thread.sleep(5000);
   
}	

	////////////////////////////////
	////DECLINE BENEFITS REPORT/////
	////////////////////////////////
	@Test
	public void testDeclineBenefitsReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	
	//Click on Declined Benefits link
  String oldTab4 = driver.getWindowHandle();
  driver.findElement(By.linkText("Declined Benefits")).click();
  Thread.sleep(30000);	
	     
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab4);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab4);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab4);
	   }
      Thread.sleep(5000);
   
}		
	
	///////////////////////////
	////DEMOGRAPHIC REPORT/////
	///////////////////////////
	@Test
	public void testDemographicReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	
  //Click on Demographic link
  driver.findElement(By.linkText("Demographic")).click();
  Thread.sleep(1000);
  String oldTab5 = driver.getWindowHandle();
  driver.findElement(By.xpath("(//input[@name='Run'])[2]")).click();
  Thread.sleep(15000);
	     
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab5);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab5);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab5);
	   }
      Thread.sleep(5000);
   
}	
	
	///////////////////////////////////
	////ELECTION STATEMENTS REPORT/////
	///////////////////////////////////
	@Test
	public void testElectionStatementsReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
  
	     
  //Click on Election Statements link
  driver.findElement(By.linkText("Election Statements")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("ASAT")).clear();
  driver.findElement(By.name("ASAT")).sendKeys("01/6/2014");
  Thread.sleep(2000);
  driver.findElement(By.id("fldemail")).click();
  driver.findElement(By.id("Text1")).clear();
  driver.findElement(By.id("Text1")).sendKeys("1");
  driver.findElement(By.id("Text2")).clear();
  driver.findElement(By.id("Text2")).sendKeys("1");
  Thread.sleep(3000);
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.name("Run")).click();
  Thread.sleep(20000);   
	     
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab6);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab6);
	   }
      Thread.sleep(5000);
   
}		
	
	/////////////////////////////////
	////ENROLLMENT MATRIX REPORT/////
	/////////////////////////////////
	@Test
	public void testEnrollmentMatrixReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
     
  //Click on Enrollment Matrix link
  driver.findElement(By.linkText("Enrollment Matrix")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("C1_1")).click();
  driver.findElement(By.id("C7_1")).click();
  driver.findElement(By.id("fldgender")).click();
  driver.findElement(By.id("fldaddress")).click();
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.xpath("(//input[@name='Run'])[2]")).click();
  Thread.sleep(15000);
  
  
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab6);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab6);
	   }
      Thread.sleep(5000);
   
   
}	
	
	//////////////////////////
	////ENROLLMENT REPORT/////
	//////////////////////////
	@Test
	public void testEnrollmentReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
     
  //Click on Enrollment Report link
  driver.findElement(By.linkText("Enrollment")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("C2_1")).click();
  driver.findElement(By.id("C3_1")).click();
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.xpath("(//input[@name='Run'])[2]")).click();
  Thread.sleep(15000);
      
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab6);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab6);
	   }
      Thread.sleep(5000);
   
   
}		
	

	//////////////////////////
	////HIPAA CERTIFICATES////
	//////////////////////////
	@Test
	public void testHipaaCertificates() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
  
  //Click on HIPAA Certificates link
  driver.findElement(By.linkText("HIPAA Certificates")).click();
  Thread.sleep(2000);
  driver.findElement(By.name("ASAT")).clear();
  driver.findElement(By.name("ASAT")).sendKeys("8/25/2013");
  driver.findElement(By.id("Text1")).clear();
  driver.findElement(By.id("Text1")).sendKeys("1");
  driver.findElement(By.id("Text2")).clear();
  driver.findElement(By.id("Text2")).sendKeys("1");
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.name("Run")).click();
  Thread.sleep(15000);
  
  
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab6);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab6);
	   }
      Thread.sleep(5000);
   
   
}
	
	/////////////////////////////////
	////MONTHLY HEAD COUNT REPORT////
	/////////////////////////////////
	@Test
	public void testMonthlyHeadCountReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
  
  //Click on Monthly Head Count link
  driver.findElement(By.linkText("Monthly Head Count")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("fldgender")).click();
  driver.findElement(By.id("fldcountry")).click();
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.name("Run")).click();
  Thread.sleep(20000);
  
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab6);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab6);
	   }
      Thread.sleep(5000);
   
   
}		
	
	/////////////////////////
	////MS ACCESS EXTRACT////
	/////////////////////////
	@Test
	public void testMSAccessExtract() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
  
  ///Click on MS Access Extract link
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.linkText("MS-Access Extract")).click();
  Thread.sleep(15000);
          
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click();
	  driver.switchTo().window(oldTab6);
	  }
      catch(Throwable e){
	  System.err.println("Button not found within the time.");
	  driver.close();
	  // change focus back to old tab
	  driver.switchTo().window(oldTab6);
	   }
      Thread.sleep(5000);
   
   
}		
	
	////////////////////////////////////////
	////POPULATED IMPORT TEMPLATE REPORT////
	////////////////////////////////////////
	@Test
	public void testPopulatedImportTemplateReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
  
  //Click on Populated Import Template link
  driver.findElement(By.linkText("Populated Import Template")).click();
  driver.findElement(By.id("BENCAT_ 1")).click();
  driver.findElement(By.id("BENCAT_ 2")).click();
  driver.findElement(By.id("BENCAT_ 3")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("fldfname")).click();
  driver.findElement(By.id("fldlname")).click();
  driver.findElement(By.id("fldgender")).click();
  driver.findElement(By.id("fldstatus")).click();
  driver.findElement(By.id("fldusername")).click();
  driver.findElement(By.id("fldcity")).click();
  driver.findElement(By.id("fldstate")).click();
  driver.findElement(By.id("fldsalary")).click();
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.name("Run")).click();
  Thread.sleep(27000);
            
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
		
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click(); }
	  catch(Throwable e){
	  System.err.println("Button not found within the time.");  
	  }
  
  driver.close();
  // change focus back to old tab
  driver.switchTo().window(oldTab6);
  Thread.sleep(5000);
   
}		
		
	//////////////////////////////
	////RUN RATE REVIEW REPORT////
	//////////////////////////////
	@Test
	public void testRunRateReviewReport() throws Exception {
	 
  //Open the Tools menu and click Report Factory
  Actions builder = new Actions(driver);
  WebElement tools1 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra2 = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[4]/a"));	
  builder.moveToElement(tools1).perform();
  builder.moveToElement(cobra2).click().perform();
  Thread.sleep(2000);
   
    
  //Click on Rate Review link
  driver.findElement(By.linkText("Rate Review")).click();
  Thread.sleep(2000);
  driver.findElement(By.id("DISPLAY1")).click();
  String oldTab6 = driver.getWindowHandle();
  driver.findElement(By.name("Run")).click();
  Thread.sleep(15000);
  
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
		
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click(); }
	  catch(Throwable e){
	  System.err.println("Button not found within the time.");  
	  }
  
  driver.close();
  // change focus back to old tab
  driver.switchTo().window(oldTab6);
  Thread.sleep(5000);
   
}	
	
	
	
	/////////////////////////////
	////COBRA INITIAL NOTICE////
	/////////////////////////////
	@Test
	public void testCobraInitialNotice() throws Exception {
	 
		 //Open the Tools menu and click COBRA
	    Actions builder = new Actions(driver);
	    WebElement tools = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
	    Thread.sleep(2000);
	    WebElement cobra = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[2]/a"));	
	    builder.moveToElement(tools).perform();
	    builder.moveToElement(cobra).click().perform();
	    Thread.sleep(2000);
		    
	    driver.findElement(By.id("IN")).click();
	    driver.findElement(By.name("BtnGet")).click();
	    driver.findElement(By.id("toggleChecks")).click();
	    
	    //Scroll down for second Manage Benefits or lower  
	    JavascriptExecutor jse = (JavascriptExecutor)driver;
	    jse.executeScript("window.scrollBy(0,1050)", "");
	    
	    String oldTab6 = driver.getWindowHandle();
	    driver.findElement(By.xpath("html/body/div[1]/div[6]/table/tbody/tr/td[3]/form/table/tbody/tr/td[2]/input")).click();
	    Thread.sleep(15000);
	    
	  //Considering that there is only one tab opened in that point.
	    ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
	    newTab1.remove(oldTab6);
	    // change focus to new tab
	    driver.switchTo().window(newTab1.get(0));
	    // Do what you want here, you are in the new tab
	    driver.close();
	    // change focus back to old tab
	    driver.switchTo().window(oldTab6);
	    
   
}	
	
	/////////////////////////////
	////COBRA ELECTION NOTICE////
	/////////////////////////////
	@Test
	public void testCobraElectionNotice() throws Exception {
	 
		 //Open the Tools menu and click COBRA
	    Actions builder = new Actions(driver);
	    WebElement tools = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
	    Thread.sleep(2000);
	    WebElement cobra = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[2]/a"));	
	    builder.moveToElement(tools).perform();
	    builder.moveToElement(cobra).click().perform();
	    Thread.sleep(2000);
		    
	    driver.findElement(By.id("EN")).click();
	    driver.findElement(By.name("BtnGet")).click();
	    driver.findElement(By.id("toggleChecks")).click();
	    
	    //Scroll down for second Manage Benefits or lower  
	    JavascriptExecutor jse = (JavascriptExecutor)driver;
	    jse.executeScript("window.scrollBy(0,1050)", "");
	    
	    String oldTab6 = driver.getWindowHandle();
	    driver.findElement(By.xpath("html/body/div[1]/div[6]/table/tbody/tr/td[3]/form/table/tbody/tr/td[2]/input")).click();
	    Thread.sleep(10000);
	    
		  //Considering that there is only one tab opened in that point.
	    ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
	    newTab1.remove(oldTab6);
	    // change focus to new tab
	    driver.switchTo().window(newTab1.get(0));
	    // Do what you want here, you are in the new tab
	    driver.close();
	    // change focus back to old tab
	    driver.switchTo().window(oldTab6);
	       
}	
	
	
	
	////////////////////////////////
	////COBRA TERMINATION NOTICE////
	////////////////////////////////
	@Test
	public void testCobraTerminationNotice() throws Exception {
	 
	    //Open the Tools menu and click COBRA
	    Actions builder = new Actions(driver);
	    WebElement tools = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
	    Thread.sleep(2000);
	    WebElement cobra = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[2]/a"));	
	    builder.moveToElement(tools).perform();
	    builder.moveToElement(cobra).click().perform();
	    Thread.sleep(2000);
	    
	    //Selections on COBRA page
	    driver.findElement(By.id("TN")).click();
	    driver.findElement(By.id("ALL")).click();
	    driver.findElement(By.id("rdCoverYes")).click();
	    driver.findElement(By.name("BtnGet")).click();
	    driver.findElement(By.id("toggleChecks")).click();
	    
	    //Scroll down for second Manage Benefits or lower  
	    JavascriptExecutor jse = (JavascriptExecutor)driver;
	    jse.executeScript("window.scrollBy(0,1050)", "");
	    	    
	    String oldTab6 = driver.getWindowHandle();
	    driver.findElement(By.xpath("html/body/div[1]/div[6]/table/tbody/tr/td[3]/form/table/tbody/tr/td[2]/input")).click();
	    Thread.sleep(10000);
	    
	  	    
		  //Considering that there is only one tab opened in that point.
	    ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
	    newTab1.remove(oldTab6);
	    // change focus to new tab
	    driver.switchTo().window(newTab1.get(0));
	    // Do what you want here, you are in the new tab
	    driver.close();
	    // change focus back to old tab
	    driver.switchTo().window(oldTab6);
	    
}	
	
	
	////////////////////////////
	////COBRA PREMIUM NOTICE////
	////////////////////////////
	@Test
	public void testCobraPremiumNotice() throws Exception {
	 
		 //Open the Tools menu and click COBRA
	    Actions builder = new Actions(driver);
	    WebElement tools = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
	    Thread.sleep(2000);
	    WebElement cobra = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[2]/a"));	
	    builder.moveToElement(tools).perform();
	    builder.moveToElement(cobra).click().perform();
	    Thread.sleep(2000);
	    
	    driver.findElement(By.linkText("Premium Notice")).click();
	    new Select(driver.findElement(By.name("BILLMONTH"))).selectByVisibleText("7/2014");
	    
	    String oldTab6 = driver.getWindowHandle();
	    driver.findElement(By.name("Run")).click();
	    Thread.sleep(7000);
	    
	    //Considering that there is only one tab opened in that point.
	    ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
	    newTab1.remove(oldTab6);
	    // change focus to new tab
	    driver.switchTo().window(newTab1.get(0));
	    // Do what you want here, you are in the new tab
	    driver.close();
	    // change focus back to old tab
	    driver.switchTo().window(oldTab6);
}	
	
	
	
	////////////////////////////////
	////RUN SECURE UPLOAD CENTER////
	////////////////////////////////
	@Test
	public void testSecureUploadCenter() throws Exception {
	 
	//Open the Tools menu and click Secure Upload Center
  Actions builder = new Actions(driver);
  WebElement tools = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/a"));
  Thread.sleep(2000);
  WebElement cobra = driver.findElement(By.xpath("html/body/div[1]/div[1]/div[3]/table/tbody/tr/td[2]/ul/li[2]/ul/li[6]/a"));	
  builder.moveToElement(tools).perform();
  builder.moveToElement(cobra).click().perform();
  String oldTab6 = driver.getWindowHandle();
  Thread.sleep(2000);
  
  //Secure Upload Center
  //WebElement fileInput = driver.findElement(By.name("file1"));
  //fileInput.sendKeys("C:\\Users\\whamidy\\Desktop\\git-model@2x.png");
  
  driver.findElement(By.name("file1")).sendKeys("C:\\Users\\whamidy\\Desktop\\git-model@2x.png");
  Thread.sleep(3000);
  driver.findElement(By.name("tbComments")).clear();
  driver.findElement(By.name("tbComments")).sendKeys("Test");
  driver.findElement(By.name("Sub1")).click();
  Thread.sleep(20000);
  
  //Considering that there is only one tab opened in that point.
  ArrayList<String> newTab1 = new ArrayList<String>(driver.getWindowHandles());
  newTab1.remove(oldTab6);
  // change focus to new tab
  driver.switchTo().window(newTab1.get(0));
  // Do what you want here, you are in the new tab
  try{
	  	
	  WebDriverWait wait = new WebDriverWait(driver,5);
	  WebElement Reportrunningalready = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	  
		
	  System.out.println("Button found. Clicking it now.");	
	  driver.findElement(By.name("btn")).click(); }
	  catch(Throwable e){
	  System.err.println("Button not found within the time.");  
	  }
  
  driver.close();
  // change focus back to old tab
  driver.switchTo().window(oldTab6);
  Thread.sleep(5000);
  
	}
  
  
  
  
	
	
	
	
	
	
  
/////EMPLOYEEE TESTS
 
    ////////////
	//EE LOGIN//
	////////////
	  
	@Test
	public void testEELogin() throws Exception {
	driver.get(url1);
	Thread.sleep(2000);
	//Maximize window
	driver.manage().window().maximize();
	
    driver.findElement(By.name("TBSiteID")).clear();
    driver.findElement(By.name("TBSiteID")).sendKeys(siteid);
	driver.findElement(By.id("TBUserName")).clear();
    driver.findElement(By.id("TBUserName")).sendKeys(employeeinitnew);
    driver.findElement(By.name("TBPassword")).clear();
    driver.findElement(By.name("TBPassword")).sendKeys(employeepass);
    driver.findElement(By.name("Login")).click();
    Thread.sleep(2000);
    log.info("LOGIN SUCCESSFUL");
    Thread.sleep(1500);
    	    
    
    //PASSWORD CHANGE REQUIRED
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,2);
	WebElement chgpw1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("PWD")));
	WebElement chgpw2 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("PWD2")));
	
	System.out.println("Change password found. Clicking it now.");	
	driver.findElement(By.name("PWD")).sendKeys(employeepassnew);
	driver.findElement(By.name("PWD2")).sendKeys(employeepassnew);
	driver.findElement(By.name("Confirm")).click();
	}
    catch(Throwable e){
	System.err.println("Change password not found within the time.");
	Thread.sleep(2000);
    }
    
    //CLICK OK ON SUCCESS POPUP
    try{
    	
	WebDriverWait wait = new WebDriverWait(driver,2);
	WebElement successpop1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(".//*[@id='MB_content']/table/tbody/tr[2]/td/form/table/tbody/tr/td/input")));
		
	System.out.println("Popup found. Clicking it now.");	
	driver.findElement(By.xpath(".//*[@id='MB_content']/table/tbody/tr[2]/td/form/table/tbody/tr/td/input")).click();
	}
    catch(Throwable e){
	System.err.println("Popup not found within the time.");
    Thread.sleep(1500);
    }

   
	 //LOOK FOR BTGACCEPT
    try{
    	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,2);
	WebElement IAgree1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btAccept")));
	
	System.out.println("I Agree button found. Clicking it now.");	
	driver.findElement(By.name("btAccept")).click();
	}
    catch(Throwable e){
	System.err.println("I Agree button was not found within the time.");
	   }
	
    
    //LOOK FOR BTGACCEPT
    try{
    	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,2);
	WebElement IAgree1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btGAAccept")));
	
	System.out.println("I Agree 2 button found. Clicking it now.");	
	driver.findElement(By.name("btGAAccept")).click();
	}
    catch(Throwable e){
	System.err.println("I Agree 2 button was not found within the time.");
	   }
    Thread.sleep(2000);
    
	}
    
	/////////////////
	//ADD DEPENDENT//
	/////////////////

	@Test
	public void testEEAddDependent() throws Exception {
		
	long timeSeed3 = System.nanoTime(); // to get the current date time value
	double randSeed3 = Math.random() * 1000; // random number generation
	long midSeed3 = (long) (timeSeed3 * randSeed3);  // mixing up the time and rand number variable timeSeed will be unique.

	String s3 = midSeed3 + "";
	String subStr3 = s3.substring(0, 9);

	int myRandomNum3 = Integer.parseInt(subStr3);    // integer value

		
		
	//Add A Family Member
    driver.findElement(By.linkText("Add A Family Member")).click();
    Thread.sleep(2000);
    driver.findElement(By.name("TBNEWSSN")).sendKeys(""+myRandomNum3);
    driver.findElement(By.name("TBFname")).sendKeys("Connor");
    driver.findElement(By.name("TBLname")).sendKeys("Bird");
    driver.findElement(By.name("TBBdate")).sendKeys("11/10/2001");
    new Select(driver.findElement(By.name("cmbGender"))).selectByIndex(1);
    new Select(driver.findElement(By.name("CmbStatus"))).selectByIndex(1);
    driver.findElement(By.name("TBADDR1")).clear();
    driver.findElement(By.name("TBADDR1")).sendKeys("1234 Staten St");
    driver.findElement(By.name("TBCity")).clear();
	driver.findElement(By.name("TBCity")).sendKeys("San Diego");
	new Select(driver.findElement(By.name("cmbState"))).selectByIndex(5);
	driver.findElement(By.name("TBPostal")).clear();
	driver.findElement(By.name("TBPostal")).sendKeys("92129");
    driver.findElement(By.name("btUpdate")).click();
    Thread.sleep(1000);
	driver.findElement(By.name("btn")).click();
	Thread.sleep(3000);
  
    log.info("ADD DEPENDENT SUCCESSFUL");

}
	/////////////////
	////ADD SPOUSE///
	/////////////////
	
	@Test
	public void testEEAddSpouse() throws Exception {
	
	
	long timeSeed4 = System.nanoTime(); // to get the current date time value
	double randSeed4 = Math.random() * 1000; // random number generation
	long midSeed4 = (long) (timeSeed4 * randSeed4);  // mixing up the time and rand number variable timeSeed will be unique.

	String s4 = midSeed4 + "";
	String subStr4 = s4.substring(0, 9);

	int myRandomNum4 = Integer.parseInt(subStr4);    // integer value	
	
    
	//Add Spouse
	driver.findElement(By.linkText("Add Spouse")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("TBNEWSSN")).sendKeys(""+myRandomNum4);
	driver.findElement(By.name("TBFname")).sendKeys("Jenny");
	driver.findElement(By.name("TBLname")).sendKeys("Baker");
	driver.findElement(By.name("TBBdate")).sendKeys("11/10/1950");
	new Select(driver.findElement(By.name("cmbGender"))).selectByIndex(2);
	new Select(driver.findElement(By.name("CmbStatus"))).selectByIndex(2);
	driver.findElement(By.name("TBADDR1")).clear();
	driver.findElement(By.name("TBADDR1")).sendKeys("1234 Staten St");
	driver.findElement(By.name("TBCity")).clear();
	driver.findElement(By.name("TBCity")).sendKeys("San Diego");
	new Select(driver.findElement(By.name("cmbState"))).selectByIndex(5);
	driver.findElement(By.name("TBPostal")).clear();
	driver.findElement(By.name("TBPostal")).sendKeys("92129");
	driver.findElement(By.name("btUpdate")).click();
	Thread.sleep(1000);
	driver.findElement(By.name("btn")).click();
	Thread.sleep(4000);
    
	log.info("ADD SPOUSE SUCCESSFUL");
  }

	/////////////////////////////////
	//////PROCEED TO MY BENEFITS/////
	/////////////////////////////////
	
	@Test
	public void testProceedToMyBenefits() throws Exception {
	//Proceed To My Benefits
	driver.findElement(By.name("BENEFITS")).click();
	Thread.sleep(3000);
	log.info("PROCEED TO MY BENEFITS");
  }
	

	/////////////////
	//ADD MEDICAL////
	/////////////////
	
	@Test
	public void testEEAddMedical() throws Exception {
		
	driver.findElement(By.xpath("//form[@id='submitform112293823']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(3000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
	
		
	//PICK A PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("chkSjwiD20131024133247")));
	
	System.out.println("Checkbox1 found. Clicking it now.");	
	driver.findElement(By.id("chkSjwiD20131024133247")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox1 was not found within the time.");
	}
	
	
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox2 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("chkMaQrf20101101103803")));
	
	System.out.println("Checkbox2 found. Clicking it now.");	
	driver.findElement(By.id("chkMaQrf20101101103803")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox2 was not found within the time.");
	}
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnbutton2 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
	
	
	//I agree button
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctn1button = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
	Thread.sleep(2000);
	
	
	//CLICK OK ON POPUP
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement okbtnpop1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	
	System.out.println("OK button on popup found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
	catch(Throwable e){
	System.err.println("OK button on popup was not found within the time.");
	}
	Thread.sleep(2000);
	log.info("ADD MEDICAL SUCCESSFUL");
	
	}
	
	
	/////////////////
	//ADD DENTAL/////
	/////////////////
	
	@Test
	public void testEEAddDental() throws Exception {
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,350)", "");
	
	//Dental Benefits
	driver.findElement(By.xpath("//form[@id='submitform110009356']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnDentalbutton1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
		
	//Check option
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(".//*[@id='chkoGG2W20100920135344']")));
	
	System.out.println("Check box found. Clicking it now.");	
	driver.findElement(By.xpath(".//*[@id='chkoGG2W20100920135344']")).click();
	}
	catch(Throwable e){
	System.err.println("Check box was not found within the time.");
	}
	Thread.sleep(1000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctn1button = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
	
	//I AGREE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement Iagree1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
			
	
	//I AGREE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement Iagree1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	
	System.out.println("Ok button found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
	catch(Throwable e){
	System.err.println("Ok button was not found within the time.");
	}
	Thread.sleep(2000);
	log.info("ADD DENTAL SUCCESSFUL");
	
	}
	
	
	
	
	
	/////////////////
	///ADD VISION////
	/////////////////
	
	@Test
	public void testEEAddVision() throws Exception {
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,450)", "");
	
	//Vision Benefits
	driver.findElement(By.xpath("//form[@id='submitform110009041']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnVisionbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
	
	//PICK A PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkvisionplans = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(".//*[@id='chkoGG2W20100920135344']")));
	
	System.out.println("Check box found. Clicking it now.");	
	driver.findElement(By.xpath(".//*[@id='chkoGG2W20100920135344']")).click();
	}
	catch(Throwable e){
	System.err.println("Check box was not found within the time.");
	}
	Thread.sleep(1000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctn1button = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
	
	
	//I AGREE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement Iagree1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
			
	
	//OK BUTTON POPUP
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement OKbtn11 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	
	System.out.println("Ok button found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
	catch(Throwable e){
	System.err.println("Ok button was not found within the time.");
	}
	Thread.sleep(2000);
	log.info("ADD VISION SUCCESSFUL");
		
	}
	
	
	
	
	/////////////////////////
	///ADD CORE LIFE/AD&D////
	/////////////////////////
	
	@Test
	public void testEEAddCoreLife() throws Exception {
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,680)", "");
	
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform110038560']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnCoreLifebutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
		
	//PICK A PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkgbqyu20131104135446")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkgbqyu20131104135446")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
		
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnbutton1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(3000);
	
	//Primary Beneficiaries
	driver.findElement(By.name("NameP1")).clear();
	driver.findElement(By.name("NameP1")).sendKeys("John Bryant");
	driver.findElement(By.name("RelationP1")).clear();
	driver.findElement(By.name("RelationP1")).sendKeys("Son");
	driver.findElement(By.name("PercentP1")).clear();
	driver.findElement(By.name("PercentP1")).sendKeys("50");
	driver.findElement(By.name("NameP2")).clear();
	driver.findElement(By.name("NameP2")).sendKeys("Kelly Bryant");
	driver.findElement(By.name("RelationP2")).clear();
	driver.findElement(By.name("RelationP2")).sendKeys("Wife");
	driver.findElement(By.name("PercentP2")).clear();
	driver.findElement(By.name("PercentP2")).sendKeys("50");
	
	//Contingent Beneficiaries
	driver.findElement(By.name("NameC1")).clear();
	driver.findElement(By.name("NameC1")).sendKeys("John Wayne");
	driver.findElement(By.name("RelationC1")).clear();
	driver.findElement(By.name("RelationC1")).sendKeys("Brother");
	driver.findElement(By.name("PercentC1")).clear();
	driver.findElement(By.name("PercentC1")).sendKeys("50");
	driver.findElement(By.name("NameC2")).clear();
	driver.findElement(By.name("NameC2")).sendKeys("Sally Sue");
	driver.findElement(By.name("RelationC2")).clear();
	driver.findElement(By.name("RelationC2")).sendKeys("Sister");
	driver.findElement(By.name("PercentC2")).clear();
	driver.findElement(By.name("PercentC2")).sendKeys("50");
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	
	//I AGREE
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement IAgree13 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("SUBMIT")));
	
	System.out.println("I agree button found. Clicking it now.");	
	driver.findElement(By.name("SUBMIT")).click();
	}
	catch(Throwable e){
	System.err.println("I agree button was not found within the time.");
	}
	Thread.sleep(2000);
	
	
	
	//OK BUTTON POPUP
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement OKbtn12 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	
	System.out.println("Ok button found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
	catch(Throwable e){
	System.err.println("Ok button was not found within the time.");
	}
	Thread.sleep(2000);
	log.info("ADD CORE LIFE AD&D SUCCESSFUL");
	
	}
	
	
	//////////////////////////////
	///ADD CORE DEPENDENT LIFE////
	//////////////////////////////
	
	@Test
	public void testEEAddCoreDependentLife() throws Exception {
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,880)", "");
	
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform116163354']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnCoreDepbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
		
	//PICK PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkboxcordep1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkgbqyu20131104135446")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkgbqyu20131104135446")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
	
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	
	
	//CHECK IF THERE IS A REASON DROP DOWN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkforreasoncd = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("REASONCD")));
	
	System.out.println("Reason drop down found. Clicking it now.");	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	}
	catch(Throwable e){
	System.err.println("Reason drop down was not found within the time.");
	}
	
	//I AGREE BUTTON
	try{
		
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkforctn1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("SUBMIT")));
	
	System.out.println("I Agree button found. Clicking it now.");	
	driver.findElement(By.name("SUBMIT")).click();
	}
	catch(Throwable e){
	System.err.println("I Agree button was not found within the time.");
	}
	Thread.sleep(1000);
		
	//OK BUTTON POPUP
	try{
		
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkforOKpopup21 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	
	System.out.println("OK button found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
	catch(Throwable e){
	System.err.println("OK button was not found within the time.");
	}
	Thread.sleep(2000);
	log.info("ADD CORE DEPENDENT LIFE SUCCESSFUL");
	
	}
	
	
	
	//////////////////////////////
	///ADD LONG TERM DISABILITY///
	//////////////////////////////
	
	@Test
	public void testEEAddLongTermDisability() throws Exception {
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,980)", "");
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform110030792']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnLTDisabbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
	
	//PICK PLAN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement addltdcheck1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkgbqyu20131104135446")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkgbqyu20131104135446")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
	
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	
	
	//CHECK IF THERE IS A REASON DROP DOWN
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ltdcheckreason = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("REASONCD")));
	
	System.out.println("Reason drop down found. Clicking it now.");	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	}
	catch(Throwable e){
	System.err.println("Reason drop down was not found within the time.");
	}
	
	//I AGREE BUTTON
	try{
		
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkforIagree01 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("SUBMIT")));
	
	System.out.println("I Agree button found. Clicking it now.");	
	driver.findElement(By.name("SUBMIT")).click();
	}
	catch(Throwable e){
	System.err.println("I Agree button was not found within the time.");
	}
	Thread.sleep(1000);
		
	//OK BUTTON POPUP
	try{
		
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkltdokpop1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("btn")));
	
	System.out.println("OK button found. Clicking it now.");	
	driver.findElement(By.name("btn")).click();
	}
	catch(Throwable e){
	System.err.println("OK button was not found within the time.");
	}
	Thread.sleep(2000);
	log.info("ADD LONG TERM DISABILITY SUCCESSFUL");
	
	}
	
	
	
	//////////////////////////////
	//////////ADD CORE STD////////
	//////////////////////////////
	
	@Test
	public void testEEAddCoreSTD() throws Exception {
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,450)", "");
	
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform115029826']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnCoreSTDbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
	
	//Pick Plan
	try{
	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkTradd20131104134004")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkTradd20131104134004")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
	
	//driver.findElement(By.id("chkTradd20131104134004")).click();
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(5000);
	log.info("ADD CORE STD SUCCESSFUL");
	
	}
	
	
	////////////////////////////
	////////ADD CORE LTD////////
	////////////////////////////
	
	@Test
	public void testEEAddCoreLTD() throws Exception {
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,950)", "");
	
	//Core Life ADD Benefits
	driver.findElement(By.xpath("//form[@id='submitform112600696']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnCoreLTDbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
		
		
	//Pick Plan
	try{
	
	//Waits for 30 seconds till the element, in your case the submit button, with name "btAccept2" is visible
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement checkbox = wait.until(ExpectedConditions.visibilityOfElementLocated(By.name("chkNCn9W20131104134627")));
	
	System.out.println("Checkbox found. Clicking it now.");	
	driver.findElement(By.name("chkNCn9W20131104134627")).click();
	}
	catch(Throwable e){
	System.err.println("Checkbox was not found within the time.");
	}
	
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	
	new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(5000);
	log.info("ADD CORE LTD SUCCESSFUL");
	
	}
	
	
	
	////////////////////////////
	////ADD HEALTH CARE FSA/////
	////////////////////////////
	
	@Test
	public void testEEAddHealthCareFSA() throws Exception {
		
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,850)", "");
	
	//HealthCare FSA Benefits
	driver.findElement(By.xpath("//form[@id='submitform112293795']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnAddHealthCareFSA = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
	
	
	driver.findElement(By.name("02pzv20131120115631VOLUME")).sendKeys("2500");
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	log.info("ADD HEALTH CARE FSA SUCCESSFUL");
	
	}
	
	
	///////////////////////////////
	////ADD DEPENDENT CARE FSA/////
	///////////////////////////////
	
	@Test
	public void testEEAddDependentCareFSA() throws Exception {
		
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,1050)", "");
	
	//HealthCare FSA Benefits
	driver.findElement(By.xpath("//form[@id='submitform111735757']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnDependentCareFSAbutton3 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
	
	
	driver.findElement(By.name("P6w97T20101101135938VOLUME")).sendKeys("2500");
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	log.info("ADD DEPENDENT CARE FSA SUCCESSFUL");
	
	}
	
	
	
	//////////////////////////////
	////ADD VOLUNTARY EE LIFE/////
	//////////////////////////////
	
	@Test
	public void testEEAddVoluntaryEELife() throws Exception {
		
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,1150)", "");
	
	//HealthCare FSA Benefits
	driver.findElement(By.xpath("//form[@id='submitform110450913']//ul[@class='menuUL opMenu']")).click();
	driver.findElement(By.linkText("Add or View Plan/Options: New Hire")).click();
	Thread.sleep(2000);
	
	//CONTINUE BUTTON
	try{
	
	WebDriverWait wait = new WebDriverWait(driver,5);
	WebElement ctnAddVoluntaryEElife = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("submitBtn")));
	
	System.out.println("Continue button found. Clicking it now.");	
	driver.findElement(By.id("submitBtn")).click();
	}
	catch(Throwable e){
	System.err.println("Continue button was not found within the time.");
	}
	Thread.sleep(2000);
	
	
	new Select(driver.findElement(By.name("WJmNQ20131104140548VOLUME"))).selectByIndex(2);
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);
	
	//Primary Beneficiaries
	driver.findElement(By.name("NameP1")).clear();
	driver.findElement(By.name("NameP1")).sendKeys("Suzie Calvin");
	driver.findElement(By.name("RelationP1")).clear();
	driver.findElement(By.name("RelationP1")).sendKeys("Daughter");
	driver.findElement(By.name("PercentP1")).clear();
	driver.findElement(By.name("PercentP1")).sendKeys("50");
	driver.findElement(By.name("NameP2")).clear();
	driver.findElement(By.name("NameP2")).sendKeys("Kelly Crile");
	driver.findElement(By.name("RelationP2")).clear();
	driver.findElement(By.name("RelationP2")).sendKeys("Daughter");
	driver.findElement(By.name("PercentP2")).clear();
	driver.findElement(By.name("PercentP2")).sendKeys("50");
	
	//Contingent Beneficiaries
	driver.findElement(By.name("NameC1")).clear();
	driver.findElement(By.name("NameC1")).sendKeys("John Wayne");
	driver.findElement(By.name("RelationC1")).clear();
	driver.findElement(By.name("RelationC1")).sendKeys("Brother");
	driver.findElement(By.name("PercentC1")).clear();
	driver.findElement(By.name("PercentC1")).sendKeys("50");
	driver.findElement(By.name("NameC2")).clear();
	driver.findElement(By.name("NameC2")).sendKeys("Sally Sue");
	driver.findElement(By.name("RelationC2")).clear();
	driver.findElement(By.name("RelationC2")).sendKeys("Sister");
	driver.findElement(By.name("PercentC2")).clear();
	driver.findElement(By.name("PercentC2")).sendKeys("50");
	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(2000);

	driver.findElement(By.name("SUBMIT")).click();
	Thread.sleep(3000);
	log.info("Add Voluntary EE Life successful");
	
	}
	
			
	//////////////////////////////
	///////ELECTION SUMMARY///////
	//////////////////////////////
	
	@Test
	public void testElectionSummary() throws Exception {
		
	//Save Parent Window Handle
    String parentHandle = driver.getWindowHandle(); 
    
    //Click On Election Summary
    driver.findElement(By.xpath(".//*[@id='header']/div[3]/table/tbody/tr/td[2]/ul/li[1]/a")).click();
    Thread.sleep(8000); 
  
    //Switch WebDriver Focus To Pop Up Window Handle
    for (String winHandle : driver.getWindowHandles()) {
        driver.switchTo().window(winHandle); 
    }

    //Close New Window
    driver.close(); 
    
    //Switch Back To Original Window 
    driver.switchTo().window(parentHandle); 
	    
	log.info("ELECTION SUMMARY SUCCESSFUL");
	}
	
	

	///////////////////////////////
	//////DECLINE MEDICAL PLAN//////
	///////////////////////////////
	
	@Test
	public void testEEDeclineMedicalPlan() throws Exception {
	
    driver.findElement(By.xpath("//form[@id='submitform112293823']//ul[@class='menuUL opMenu']")).click();
    driver.findElement(By.linkText("Decline Benefit: New Hire")).click();
    Thread.sleep(2000);
    new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
    driver.findElement(By.name("SUBMIT")).click();
    Thread.sleep(2000);
    driver.findElement(By.name("btn")).click();
    Thread.sleep(2000);
       
      
	log.info("DECLINE MEDICAL SUCCESSFUL");
	
	}
   
	///////////////////////////////
	//////DECLINE DENTAL PLAN//////
	///////////////////////////////
	
	@Test
	public void testEEDeclineDentalPlan() throws Exception {
	
    driver.findElement(By.xpath("//form[@id='submitform112293823']//ul[@class='menuUL opMenu']")).click();
    driver.findElement(By.linkText("Decline Benefit: New Hire")).click();
    Thread.sleep(2000);
    new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
    driver.findElement(By.name("SUBMIT")).click();
    Thread.sleep(2000);
    driver.findElement(By.name("btn")).click();
    Thread.sleep(2000);
       
    log.info("DECLINE DENTAL SUCCESSFUL");
	
	}
	
	///////////////////////////////
	//////DECLINE VISION PLAN//////
	///////////////////////////////
	
	@Test
	public void testEEDeclineVisionPlan() throws Exception {
		
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse = (JavascriptExecutor)driver;
	jse.executeScript("window.scrollBy(0,-1150)", "");
	Thread.sleep(2000);
	
	//Scroll down for second Manage Benefits or lower  
	JavascriptExecutor jse2 = (JavascriptExecutor)driver;
	jse2.executeScript("window.scrollBy(0,450)", "");
	
    driver.findElement(By.xpath("//form[@id='submitform110009041']//ul[@class='menuUL opMenu']")).click();
    driver.findElement(By.linkText("Decline Benefit: New Hire")).click();
    Thread.sleep(2000);
    new Select(driver.findElement(By.name("REASONCD"))).selectByIndex(1);
    driver.findElement(By.name("SUBMIT")).click();
    Thread.sleep(2000);
    driver.findElement(By.name("btn")).click();
    Thread.sleep(2000);
              
	log.info("DECLINE VISION SUCCESSFUL");
	
	}
	
	
		

  
  @AfterSuite
   public void tearDown() throws Exception {
    driver.quit();
    String verificationErrorString = verificationErrors.toString();
    if (!"".equals(verificationErrorString)) {
      Assert.fail(verificationErrorString);
    }
  }

  private boolean isElementPresent(By by) {
    try {
      driver.findElement(by);
      return true;
    } catch (NoSuchElementException e) {
      return false;
    }
  }

  private boolean isAlertPresent() {
    try {
      driver.switchTo().alert();
      return true;
    } catch (NoAlertPresentException e) {
      return false;
    }
  }

  private String closeAlertAndGetItsText() {
    try {
      Alert alert = driver.switchTo().alert();
      String alertText = alert.getText();
      if (acceptNextAlert) {
        alert.accept();
      } else {
        alert.dismiss();
      }
      return alertText;
    } finally {
      acceptNextAlert = true;
    }
  }
}