package utilities;

import java.io.File;
import java.io.FileInputStream;
import org.apache.poi.xssf.usermodel.*;

public class ReadExcelDataFile
{
	XSSFWorkbook wb ;
	XSSFSheet sheet ;
		
	public ReadExcelDataFile(String path)
	{
		
		try 
		{
			File src = new File(path);
			FileInputStream fis = new FileInputStream(src);
			wb = new XSSFWorkbook(fis);
			//sheet = wb.getSheetAt(0);			
		} 
		catch (Exception e)
		{
			System.out.println(e.getMessage());	
		}
	}
	
	public String getData(int sheetNumber , int row , int column)
	{
		sheet = wb.getSheetAt(sheetNumber);
		String data = sheet.getRow(row).getCell(column).getStringCellValue();
		return data;
	}
	

}