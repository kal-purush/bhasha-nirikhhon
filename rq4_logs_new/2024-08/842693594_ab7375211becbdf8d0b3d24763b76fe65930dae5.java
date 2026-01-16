package com.api.ReusableUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class UserExcelReader {
	
	public static int totalRow;

	public static List<Map<String, String>> getData(String excelFilePath, String sheetName)
			throws InvalidFormatException, IOException {

		Workbook workbook = WorkbookFactory.create(new File(excelFilePath));
		org.apache.poi.ss.usermodel.Sheet sheet = workbook.getSheet(sheetName);
		workbook.close();
		return readSheet(sheet);
	}

	private static List<Map<String, String>> readSheet(Sheet sheet) {

	    totalRow = sheet.getLastRowNum();

	    List<Map<String, String>> excelRows = new ArrayList<>();
	    DataFormatter dataFormatter = new DataFormatter();

	    for (int currentRow = 1; currentRow <= totalRow; currentRow++) {
	        Row row = sheet.getRow(currentRow);

	        if (row != null) {
	            LinkedHashMap<String, String> columnMapdata = new LinkedHashMap<>();

	            int totalColumn = row.getLastCellNum();

	            for (int currentColumn = 0; currentColumn < totalColumn; currentColumn++) {
	                Cell cell = row.getCell(currentColumn);

	                if (cell != null) {
	                    String columnHeaderName = sheet.getRow(sheet.getFirstRowNum()).getCell(currentColumn)
	                            .getStringCellValue();

	                    // Use a DataFormatter to handle different cell types
	                    String cellValue = dataFormatter.formatCellValue(cell);

	                    columnMapdata.put(columnHeaderName, cellValue);
	                } else {
	                    // Handle the case when the cell is null, for example, by adding an empty string
	                    columnMapdata.put("Column" + currentColumn, "");
	                }
	            }

	            excelRows.add(columnMapdata);
	        }
	    }

	    return excelRows;
	}

}