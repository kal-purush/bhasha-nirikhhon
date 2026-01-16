package com.reflection5;

import java.io.FileInputStream;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelSheetDemo {

	public static void main(String[] args) {
		getSheetCount();

	}
	
	private static void getSheetCount()
	{
		FileInputStream fin=null;
		Workbook wb=null;
		Sheet sh=null;
		try
		{
			fin=new FileInputStream(".\\DataFiles\\MethodNames.xlsx");
			wb=new XSSFWorkbook(fin);
			int shcount=wb.getNumberOfSheets();
			for(int s=0;s<shcount;s++)
			{
				String name=wb.getSheetName(s);
				System.out.println(name);
				
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		finally
		{
			try
			{
				fin.close();
				wb.close();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}