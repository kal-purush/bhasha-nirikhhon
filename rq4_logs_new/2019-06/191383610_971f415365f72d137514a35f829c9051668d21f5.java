package qa.com.datadrivenframework3;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

import qa.com.excel.utilities3.Xls_Reader1;

public class ExcelDataDriven {

	public static void main(String[] args) throws InterruptedException {

		
		//excel data
		Xls_Reader1 db3 = new Xls_Reader1("E:/workbook.xlsx");
		String FirstName =db3.getCellData("Deepak", "First name", 2);
		System.out.println("FirstName :-" + FirstName );
		
		String surname =db3.getCellData("Deepak", "Sirname", 2);
		System.out.println("FirstName :-" + surname );
		
		
		//webdiver code
		
		System.setProperty("webdriver.chrome.driver","E:\\selenium\\chromedriver_win32\\chromedriver.exe" );
		WebDriver driver=new ChromeDriver();
		
		driver.get("https://mail.rediff.com/cgi-bin/login.cgi");
	   driver.findElement(By.xpath("//*[@id='login1']")).sendKeys(FirstName);
	   driver.findElement(By.xpath("//*[@id='password']")).sendKeys(surname);
		

	}

}