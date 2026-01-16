package com.EcxelPOI;

import java.io.FileOutputStream;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class CityName 
{

	public static void main(String[] args) 
	{
		Writecontent();

	}
	public static void Writecontent()
	{
		FileOutputStream fout=null;
		Workbook wb=null;
		Sheet sh=null;
		Row row=null;
		Cell cell=null;
		try
		{
			wb=new XSSFWorkbook();
			sh=wb.createSheet("sheet2");
			row=sh.createRow(4);
			for(int i=0;i<20;i++)
			{
				cell=row.createCell(i);
				cell.setCellValue("cityname"+i);
			}
			fout=new FileOutputStream("D:\\EXCEL\\Assignment3.xlsx");
			wb.write(fout);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				wb.close();
				fout.close();
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}


	}

}