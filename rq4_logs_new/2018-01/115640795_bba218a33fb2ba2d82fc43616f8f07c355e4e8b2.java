package po;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import po.utils.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class WorkWithExcelFiles {
    private static String TEST_DATA = "src/main/resources/TestData.xlsx";
    private static String TEST_OUT_PUT = "src/main/resources/TestOutPut.xlsx";

    //https://tproger.ru/translations/how-to-read-write-excel-file-java-poi-example/
    public static String loadSortByText(String SHEET, int INDEX) {
        XSSFWorkbook testData = null;
        testData = getDataFrom(TEST_DATA, testData);
        XSSFRow row = testData.getSheet(SHEET).getRow(INDEX);
        Logger.info("Row number " + (INDEX + 1) + " exist on " + SHEET + " in TestData.xlsx");
        String KIND_OF_SORT = row.getCell(0).getStringCellValue();
        closeWorkBook(testData);
        return KIND_OF_SORT;
    }

    public static void loadInExcel(String SHEET, int INDEX, String PRICE_FROM_SITE) {
        XSSFWorkbook wb = null;
        wb = getDataFrom(TEST_OUT_PUT, wb);
        if (wb == null) {
            wb = getDataFrom(TEST_DATA, wb);
        }
        XSSFRow row = wb.getSheet(SHEET).getRow(INDEX);
        Logger.info("Row number " + (INDEX + 1) + " exist on " + SHEET);
        XSSFCell cell = row.createCell(row.getLastCellNum());
        cell.setCellValue( PRICE_FROM_SITE);
        FileOutputStream fileOut = null;
        fileOut = getFileOutInstance(fileOut);
        try {
            wb.write(fileOut);
            fileOut.flush();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        closeWorkBook(wb);
    }

    private static XSSFWorkbook getDataFrom(String ANY_XLSX_FILE, XSSFWorkbook WorkBook) {
        try {
            WorkBook = new XSSFWorkbook(new FileInputStream(ANY_XLSX_FILE));
            Logger.info(ANY_XLSX_FILE + " was found");
        } catch (IOException e) {
            Logger.info(ANY_XLSX_FILE + " was NOT found");
            e.printStackTrace();
        }
        return WorkBook;
    }

    private static void closeWorkBook(XSSFWorkbook WorkBook) {
        try {
            WorkBook.close();
            Logger.info("WorkBook was closed");
        } catch (IOException e) {
            Logger.info("WorkBook was NOT closed");
            e.printStackTrace();
        }
    }

    private static FileOutputStream getFileOutInstance(FileOutputStream fileOut) {
        try {
            fileOut = new FileOutputStream(TEST_OUT_PUT);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return fileOut;
    }
}
/**
 * Found here: https://gist.github.com/madan712/3912272
 */
// import java.io.FileInputStream;
//         import java.io.FileOutputStream;
//         import java.io.IOException;
//         import java.io.InputStream;
//         import java.util.Iterator;
//         import org.apache.poi.hssf.usermodel.HSSFCell;
//         import org.apache.poi.hssf.usermodel.HSSFRow;
//         import org.apache.poi.hssf.usermodel.HSSFSheet;
//         import org.apache.poi.hssf.usermodel.HSSFWorkbook;
//         import org.apache.poi.xssf.usermodel.XSSFCell;
//         import org.apache.poi.xssf.usermodel.XSSFRow;
//         import org.apache.poi.xssf.usermodel.XSSFSheet;
//         import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//public class ReadWriteExcelFile {
//    public static void readXLSFile() throws IOException {
//        InputStream ExcelFileToRead = new FileInputStream("C:/Test.xls");
//        HSSFWorkbook wb = new HSSFWorkbook(ExcelFileToRead);
//        HSSFSheet sheet = wb.getSheetAt(0);
//        HSSFRow row;
//        HSSFCell cell;
//        Iterator rows = sheet.rowIterator();
//        while (rows.hasNext()) {
//            row = (HSSFRow) rows.next();
//            Iterator cells = row.cellIterator();
//            while (cells.hasNext()) {
//                cell = (HSSFCell) cells.next();
//                if (cell.getCellType() == HSSFCell.CELL_TYPE_STRING) {
//                    System.out.print(cell.getStringCellValue() + " ");
//                } else if (cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
//                    System.out.print(cell.getNumericCellValue() + " ");
//                } else {
//                    //U Can Handel Boolean, Formula, Errors
//                }}
//        }}
//
//    public static void writeXLSFile() throws IOException {
//        String excelFileName = "C:/Test.xls";//name of excel file
//        String sheetName = "Sheet1";//name of sheet
//        HSSFWorkbook wb = new HSSFWorkbook();
//        HSSFSheet sheet = wb.createSheet(sheetName);
//        //iterating r number of rows
//        for (int r = 0; r < 5; r++) {
//            HSSFRow row = sheet.createRow(r);
//            //iterating c number of columns
//            for (int c = 0; c < 5; c++) {
//                HSSFCell cell = row.createCell(c);
//                cell.setCellValue("Cell " + r + " " + c);
//            }}
//        FileOutputStream fileOut = new FileOutputStream(excelFileName);
//        //write this workbook to an Outputstream.
//        wb.write(fileOut);
//        fileOut.flush();
//        fileOut.close();
//    }
//    public static void readXLSXFile() throws IOException {
//        InputStream ExcelFileToRead = new FileInputStream("C:/Test.xlsx");
//        XSSFWorkbook wb = new XSSFWorkbook(ExcelFileToRead);
//        XSSFWorkbook test = new XSSFWorkbook();
//        XSSFSheet sheet = wb.getSheetAt(0);
//        XSSFRow row;
//        XSSFCell cell;
//        Iterator rows = sheet.rowIterator();
//        while (rows.hasNext()) {
//            row = (XSSFRow) rows.next();
//            Iterator cells = row.cellIterator();
//            while (cells.hasNext()) {
//                cell = (XSSFCell) cells.next();
//                if (cell.getCellType() == XSSFCell.CELL_TYPE_STRING) {
//                    System.out.print(cell.getStringCellValue() + " ");
//                } else if (cell.getCellType() == XSSFCell.CELL_TYPE_NUMERIC) {
//                    System.out.print(cell.getNumericCellValue() + " ");
//                } else {
//                    //U Can Handel Boolean, Formula, Errors
//                }}
//        }}
//
//    public static void writeXLSXFile() throws IOException {
//        String excelFileName = "C:/Test.xlsx";//name of excel file
//        String sheetName = "Sheet1";//name of sheet
//        XSSFWorkbook wb = new XSSFWorkbook();
//        XSSFSheet sheet = wb.createSheet(sheetName);
//        //iterating r number of rows
//        for (int r = 0; r < 5; r++) {
//            XSSFRow row = sheet.createRow(r);
//            //iterating c number of columns
//            for (int c = 0; c < 5; c++) {
//                XSSFCell cell = row.createCell(c);
//                cell.setCellValue("Cell " + r + " " + c);
//            }}
//        FileOutputStream fileOut = new FileOutputStream(excelFileName);
//        //write this workbook to an Outputstream.
//        wb.write(fileOut);
//        fileOut.flush();
//        fileOut.close();
//    }
//
//    public static void main(String[] args) throws IOException {
//        writeXLSFile();
//        readXLSFile();
//        writeXLSXFile();
//        readXLSXFile();
//    }
//}