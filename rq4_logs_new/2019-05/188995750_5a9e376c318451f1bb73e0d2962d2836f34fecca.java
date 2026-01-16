package exam.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.stereotype.Service;

import exam.service.IFileUploadService;

@Service
public class FileUploadServiceImpl implements IFileUploadService {

  @Override
  public List<String[]> readExcel(String path) { 
      SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd"); 
      List<String[]> list = null;
      //System.out.println("hao1");
      try { 
        //同时支持Excel 2003、2007 
        File excelFile = new File(path); //创建文件对象 
        FileInputStream is = new FileInputStream(excelFile); //文件流 
        Workbook workbook = WorkbookFactory.create(is); //这种方式 Excel 2003/2007/2010 都是可以处理的 
       // System.out.println("hao2");
        int sheetCount = workbook.getNumberOfSheets(); //Sheet的数量
        System.out.println(sheetCount);
       //存储数据容器 
        list = new ArrayList<String[]>();
        //遍历每个Sheet 
        for (int s = 0; s < sheetCount; s++) { 
          Sheet sheet = workbook.getSheetAt(s); 
          int rowCount = sheet.getPhysicalNumberOfRows(); //获取总行数
          System.out.println(rowCount);
          //遍历每一行 ，从第二行开始遍历
          for (int r = 0; r < rowCount; r++) { 
        	 //第一行跳过 
        	if(r == 0) continue;  
            Row row = sheet.getRow(r); 
            int cellCount = row.getPhysicalNumberOfCells(); //获取总列数 
            System.out.println(cellCount);
           //用来存储每行数据的容器 
            String[] model = new String[cellCount-1];
            //遍历每一列 ，从第二列开始遍历
            for (int c = 0; c < cellCount; c++) { 
              if(c == 0) continue;//第一列ID为标志列，不解析
              Cell cell = row.getCell(c); 
              int cellType = cell.getCellType();
     
              String cellValue = null; 
              switch(cellType) { 
                case Cell.CELL_TYPE_STRING: //文本 
                  cellValue = cell.getStringCellValue(); 
                //  model[c-1] = cellValue;
                  break; 
                case Cell.CELL_TYPE_NUMERIC: //数字、日期 
                  if(DateUtil.isCellDateFormatted(cell)) { 
                    cellValue = fmt.format(cell.getDateCellValue()); //日期型 
              
                  } 
                  else {                    
                    cellValue = String.valueOf(cell.getNumericCellValue()); //数字                                
                  } 
                  break; 
                case Cell.CELL_TYPE_BOOLEAN: //布尔型 
                  cellValue = String.valueOf(cell.getBooleanCellValue()); 
                  break; 
                case Cell.CELL_TYPE_BLANK: //空白 
                  cellValue = cell.getStringCellValue(); 
                  break; 
                case Cell.CELL_TYPE_ERROR: //错误 
                  cellValue = "错误"; 
                  break; 
                case Cell.CELL_TYPE_FORMULA: //公式 
                  cellValue = "错误"; 
                  break; 
                default: 
                  cellValue = "错误"; 
                  
              } 
              System.out.print(cellValue + "  "); 
              
              model[c-1] = cellValue;   //每行进行存储
            } 
            //model放入list容器中 
            list.add(model);     //按行存储
           
          } 
        } 
        is.close();     
      } 
      catch (Exception e) { 
        e.printStackTrace(); 
      }
      
      return list;  
    }
}