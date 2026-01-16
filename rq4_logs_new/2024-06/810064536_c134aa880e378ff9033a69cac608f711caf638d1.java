package assignments;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.AfterMethod;

public class Assignment3 {
	WebDriver driver;
  @Test
  public void calculatorInput() throws IOException {
	  
	  String path="C:\\Users\\anupk\\OneDrive\\Desktop\\Automation Learning\\MaturityValCalc.xlsx";
	  
	  FileInputStream inputstream=new FileInputStream(path);
	  XSSFWorkbook workbook=new XSSFWorkbook(inputstream);
	  XSSFSheet sheet=workbook.getSheet("maturityValCalculator");
	  
	  int rowCount=sheet.getLastRowNum();
	  for (int i=1;i<=rowCount;i++)
	  {
		  XSSFRow row=sheet.getRow(i);
		 // String principalValue=row.getCell(0).toString();
		  int principalValue=(int) row.getCell(0).getNumericCellValue();
		  //System.out.println("princi: "+principalValue);
		  String roiValue=row.getCell(1).toString();
		  int periodValue=(int) row.getCell(2).getNumericCellValue();
		  String periodtypeValue=row.getCell(3).toString();
		  String frequencyValue=row.getCell(4).toString();
		  
		  String [] results=validateFDCalc(principalValue,roiValue,periodValue,periodtypeValue,frequencyValue);
		  
		  double maturityValue=Double.parseDouble(results[0]);
		  String intValue=results[1];
		  double interestVal=Double.parseDouble(intValue.substring(19, 24));
		  //System.out.println("Mtvalue: "+maturityValue+" InVale: "+interestVal);
		  row.createCell(5).setCellValue(maturityValue);
		  row.createCell(6).setCellValue(interestVal);
	  }
	  FileOutputStream outputStream = new FileOutputStream(path);
      
      
      workbook.write(outputStream);
      workbook.close();
	  
  }
  @BeforeMethod
  public void beforeMethod() {
	  
	  ChromeOptions ops = new ChromeOptions();
      ops.addArguments("--disable-notifications");
      
	  driver = new ChromeDriver(ops);
	  driver.manage().window().maximize();
	  driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
	  
  }

  @AfterMethod
  public void afterMethod() {
	  driver.quit();
  }

  public String[] validateFDCalc(int principalValue,String roiValue,int periodValue,String periodtypeValue,String frequencyValue)
  {
	  driver.get("https://www.moneycontrol.com/fixed-income/calculator/state-bank-of-india-sbi/fixed-deposit-calculator-SBI-BSB001.html");
	  driver.findElement(By.xpath("//input[@id='principal']")).sendKeys(String.valueOf(principalValue));
	  driver.findElement(By.xpath("//input[@id='interest']")).sendKeys(roiValue);
	  driver.findElement(By.xpath("//input[@id='tenure']")).sendKeys(String.valueOf(periodValue));
	  WebElement tenureElemenet=driver.findElement(By.xpath("//select[contains(@id,'tenure')]"));
	  Select selectTenure=new Select(tenureElemenet);
	  selectTenure.selectByVisibleText(periodtypeValue);
	  
	  WebElement freqElement=driver.findElement(By.xpath("//select[contains(@id,'freq')]"));
	  Select selectFreq=new Select(freqElement);
	  selectFreq.selectByVisibleText(frequencyValue);
	  
	  driver.findElement(By.xpath("//a[contains(@onclick,'Mat')]")).click();
	  String matVal=driver.findElement(By.xpath("//span[contains(@id,'matval')]")).getText();
	  String intVal=driver.findElement(By.xpath("//span[contains(@id,'intval')]")).getText();
	  String []calcValue= {matVal,intVal};
	  return (calcValue);
	  
	  
  }
}