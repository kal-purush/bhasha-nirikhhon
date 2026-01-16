package com.example.demo.excel;

import java.io.File;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.servlet.view.document.AbstractXlsxView;

import com.example.demo.form.AddUsuarioForm;

public class ExcelBuilderUsuario extends AbstractXlsxView  {

	@Override
	protected void buildExcelDocument(Map<String, Object> model, Workbook workbook, HttpServletRequest request,
			HttpServletResponse response) throws Exception {

		String fileName = "test.txt";
//		response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
		
		ResourceLoader loader = new DefaultResourceLoader();
	    Resource resource = loader.getResource("classpath:static/excel/test.xlsx");
	    File file = resource.getFile();

//	    workbook = WorkbookFactory.create(file);
//	    Sheet sheet = workbook.getSheet("Hoja1");
//	    Row r = sheet.getRow(0);
//	    Cell c = r.getCell(0);
//	    c.setCellValue( "test" );
			
			
	   	AddUsuarioForm fm = (AddUsuarioForm) model.get("form");
	       	       
//	    Sheet sheet = workbook.createSheet("Spring");

//	    Row header = sheet.createRow(0);
//	    header.createCell(0).setCellValue("ID");
//	    header.createCell(1).setCellValue("Name");
	        
//	    Row courseRow = sheet.createRow(1);
//	    courseRow.createCell(0).setCellValue(fm.getLoginId());
//	    courseRow.createCell(1).setCellValue(fm.getUsername());
	    	
	}

}