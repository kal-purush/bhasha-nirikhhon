import java.io.FileInputStream;
		import java.io.FileNotFoundException;
		import java.io.IOException;

		import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
		import org.apache.poi.ss.usermodel.Workbook;
		import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class TableTest1
{

	public static void main(String[] args) 
	{
	
				System.setProperty("webdriver.chrome.driver","D:/oo/chromedriver.exe");    
			    WebDriver mydriver = new ChromeDriver();
			    mydriver.get("http://www.w3schools.com/html/html_tables.asp ");
			    mydriver.manage().window().maximize();
			    Thread.sleep(5000);
			    
			    WebElement table = mydriver.findElement(By.cssSelector("div class='w3-white w3-padding notranslate w3-padding-16' table"));
			    
			
			    String baseUrl = "file:///C:/Users/hitmachut/Downloads/codebeautify.html";
			    String filePath = "U:\\������ ����� � ����\\��� ������\\����� ��������";
			    String fileName = "ExcelTable.xlsx";
				String sheetName ="Table";
				FileInputStream inputStream = new FileInputStream(filePath+"\\"+fileName);
				Workbook excelWorkbook = new XSSFWorkbook(inputStream);	    
				Sheet excelSheet = excelWorkbook.getSheet(sheetName);
				    
	    
				    Cell currentCell= excelSheet.getRow(1).getCell(3);
				    String expectedText=currentCell.getStringCellValue();
				 
				 
				    String SearchText = mydriver.findElement(				    		
				    		By.xpath("//table/tbody/tr[1]/td[1]")).getText(); 
				    
				    verifyTableCellText(table,1,SearchText,3,expectedText);
				    


					public static String getTableCellTextByXpath(WebElement table, int searchColumn,
						String searchText, int returnColumnText) throws Exception
					{
						
						try {
							
							WebDriver mydriver = new ChromeDriver();
						    String baseUrl = "file:///C:/Users/hitmachut/Downloads/codebeautify.html";
							mydriver.get(baseUrl);
							String askedText = mydriver.findElement(By
									.xpath("//table/tbody/tr[searchColumn]/td[returnColumnText]")).getText();
							

							mydriver.quit();

							return askedText;

						     }

						catch (Exception ex) {

							ex.printStackTrace();
							return "Ooops";   }


						
					}



				    

					public static boolean verifyTableCellText(WebElement table, int searchColumn,

							String searchText, int returnColumnText, String expectedText) throws Exception

					{

						String result = getTableCellTextByXpath(table, searchColumn, searchText, returnColumnText);

						if (result != expectedText)

							return false;

						else
							return true;

			        }

	}


	}

}