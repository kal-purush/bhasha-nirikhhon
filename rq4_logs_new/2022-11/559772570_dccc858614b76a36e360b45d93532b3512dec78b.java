package com.EcxelPOI;

import java.io.FileOutputStream;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class Assignment4 
{

	public static void main(String[] args) 
	{
		Write();

	}
	private static void Write()
	{
		//Write 20FlowerNames and 20Color Names in FirstSheet and Second columns
		FileOutputStream fout=null;
		Workbook wb=null;
		Sheet sh=null;
		Row row=null;
		Cell cell=null;
		try
		{
			wb=new XSSFWorkbook();
			sh=wb.createSheet("Flowers");
			String s[]= {"flower1","flower2","flower3","flower4","flower5","flower6","flower7","flower8","flower9","flower10","flower11","flower12","flower13","flower14","flower15","flower16","flower17","flower19","flower20"};
			String s1[]= {"GREEN","RED","VIOLET","INDIGO","BLUE","YELLOW","ORANGE","WHITE","BLACK","BROWN","GREY","SAFFRON","MARRON","LAVENDER","PINK","PARROTGREEN","CRIMSON RED","PALE YELLOW","RAMA GREEN","DARK BLACK","SNOW WHITE"};
			for(int i=0;i<s.length;i++)
			{
				row=sh.createRow(i);
				cell=row.createCell(0);
				cell.setCellValue(s[i]);

				cell =row.createCell(1);
				cell.setCellValue(s1[i]);
			}



			fout=new FileOutputStream("D:\\EXCEL\\Assignment4.xlsx");
			wb.write(fout);		

		}catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				fout.close();
				wb.close();
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}


