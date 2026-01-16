package com.mycompany.exceltopdf;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Table;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class ExcelToPdfConverter {

    public static void main(String[] args) {
        String excelFilePath = "C:\\Users\\Asus\\Documents\\NetBeansProjects\\ExcelToPDF\\New Microsoft Excel Worksheet.xlsx";
        String pdfFilePath = "C:\\Users\\Asus\\Documents\\NetBeansProjects\\ExcelToPDF\\New Microsoft Excel Worksheet.pdf";

        try {
            FileInputStream excelFile = new FileInputStream(excelFilePath);
            Workbook workbook = new XSSFWorkbook(excelFile);

            PdfWriter writer = new PdfWriter(new FileOutputStream(pdfFilePath));
            PdfDocument pdf = new PdfDocument(writer);
            Document document = new Document(pdf);

            Sheet sheet = workbook.getSheetAt(0);
            Table table = new Table(sheet.getRow(0).getPhysicalNumberOfCells());

            for (Row row : sheet) {
                for (Cell cell : row) {
                    table.addCell(cell.toString());
                }
            }

            document.add(table);
            document.close();
            workbook.close();

            System.out.println("Excel failas konvertuotas į PDF sėkmingai.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}