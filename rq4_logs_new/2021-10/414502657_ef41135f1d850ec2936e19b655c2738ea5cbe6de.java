package com.two;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Dat {
	
	public static void main(String[] args) throws IOException {
		File file=new File("C:\\Users\\SABU\\MAVEN\\25.09.2021\\exc\\TASK1.xlsx");
		FileInputStream fileInputStream=new FileInputStream(file);
		Workbook workbook=new XSSFWorkbook(fileInputStream);
		Sheet sheet = workbook.getSheet("Sheet1");
		int physicalNumberOfRows = sheet.getPhysicalNumberOfRows();
		Row row = sheet.getRow(0);
		int physicalNumberOfCells = row.getPhysicalNumberOfCells();
		for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {
			Row row2 = sheet.getRow(i);
			for (int j = 0; j < row.getPhysicalNumberOfCells(); j++) {
				Cell cell = row2.getCell(j);
				int cellType = cell.getCellType();
			if (cellType==1) {
				String stringCellValue = cell.getStringCellValue();
				System.out.println(stringCellValue);
				}
			else if (cellType==0) {
				if (DateUtil.isCellDateFormatted(cell)) {
					Date dateCellValue = cell.getDateCellValue();
					SimpleDateFormat dateFormat=new SimpleDateFormat("dd-MM-yy");
					String format = dateFormat.format(dateCellValue);
					System.out.println(format);				
				}
				else {
					double numericCellValue = cell.getNumericCellValue();
					long name =(long) numericCellValue;
					String valueOf = String.valueOf(name);
					System.out.println(valueOf);
					
				}
				
			}
				
			}			
		}
		
		
		
		
		
		
	}

	
	

}