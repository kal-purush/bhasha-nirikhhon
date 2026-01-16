package ApachePOI.resource;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;

public class ExcelLogin_Mert {

    public static void main(String[] args) throws IOException {

        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("UsernameAndPassword");

        Row newRow = sheet.createRow(0);
        Cell newCell = newRow.createCell(0);
        newCell.setCellValue("Username:");
        Cell newCell1 = newRow.createCell(1);
        newCell1.setCellValue("TestTitans2@example.com");

        Row newRow1 = sheet.createRow(1);
        Cell newCell2 = newRow1.createCell(0);
        newCell2.setCellValue("Password:");
        Cell newCell3=newRow1.createCell(1);
        newCell3.setCellValue("Passw0rd");

        String path = "src/test/java/ApachePOI/resource/UsernameAndPassword.xlsx";
        FileOutputStream outputStream = new FileOutputStream(path);
        workbook.write(outputStream);
        workbook.close();
        outputStream.close();

    }
}