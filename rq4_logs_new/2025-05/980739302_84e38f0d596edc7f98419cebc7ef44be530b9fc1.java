import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class dataDriven {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		//fileinputstream object for uploading file path  file extension should be .xlsx, fis reads the data in path
		FileInputStream fis=new FileInputStream("C:\\Users\\USER\\OneDrive\\Documents\\Automation Testing\\demodata.xlsx");
		
		//accepts file input stream argument, xssfworkbook has access to object fis
		XSSFWorkbook workbook=new XSSFWorkbook(fis);
		
		//get number of sheets
		int sheets=workbook.getNumberOfSheets();
		for(int i=0;i<sheets;i++)
		{
			if(workbook.getSheetName(i).equalsIgnoreCase("testdata"));
			{
				XSSFSheet sheet=workbook.getSheetAt(i);	
				
				//identify testcases column by scanning entire first row
				Iterator<Row> rows=sheet.rowIterator();//sheet is collection of rows
				Row firstrow=rows.next(); //going to next row
				Iterator<Cell> ce=firstrow.cellIterator();//row is collection of shells
				int k=0;
				int column=0;
				while(ce.hasNext()) 
				{
					Cell value=ce.next();//going to the next cell
					//check name of cell is equal to desired column
					if(value.getStringCellValue().equalsIgnoreCase("Testcase"));
					{
						column=k;
					}
					k++;
				}
				System.out.println(column);
			}
		}
	}

}