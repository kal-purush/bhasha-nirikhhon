package submit;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;

public class excel {
    public static void main(String[] args) {
        // 엑셀 파일 생성
        Workbook workbook = new XSSFWorkbook();
        // 시트 이름 설정
        Sheet sheet = workbook.createSheet("new sheet");
        // row 추가 (0부터 시작)
        Row titleRow = sheet.createRow(0);
        // cell 추가 + 값 입력 (0부터 시작)
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue("박연지");

        // cell에 스타일 지정
        CellStyle style = workbook.createCellStyle();
        style.setVerticalAlignment(VerticalAlignment.CENTER); // 세로 중앙 정렬
        style.setAlignment(HorizontalAlignment.CENTER);//가로 중앙 정렬

        XSSFFont font = ((XSSFWorkbook)workbook).createFont();
        font.setBold(true); // 굵기 굵게 설정
        font.setFontHeight((short) (20*20)); // 글씨크기 20pt로 설정
        style.setFont(font); // 글씨 두께 굵게,사이즈 20으로 설정

        titleCell.setCellStyle(style);

        File currDir = new File("C:\\Users\\user\\IdeaProjects\\jsp"); // 현재 프로젝트 경로
        String path = currDir.getAbsolutePath();
        String fileLocation = path.substring(0, path.length()-1) + "temp.xlsx"; // 파일명 설정



    }
}