package com.example.exceltry.excel;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/entity")
public class ExportController {
    @Autowired
    private EntityService yourEntityService;

    @GetMapping("/export/excel")
    public ResponseEntity<String> exportToExcel(HttpServletResponse response) throws IOException {
        List<Map<String, Object>> data = yourEntityService.getAllEntities();

        // Create Excel workbook and sheet
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Entity Data");

        // Create header row
        Row headerRow = sheet.createRow(0);
        int colNum = 0;
        for (String columnName : data.get(0).keySet()) {
            Cell cell = headerRow.createCell(colNum++);
            cell.setCellValue(columnName);
        }

        // Create data rows
        int rowNum = 1;
        for (Map<String, Object> rowMap : data) {
            Row row = sheet.createRow(rowNum++);
            colNum = 0;
            for (Object value : rowMap.values()) {
                Cell cell = row.createCell(colNum++);
                if (value != null) {
                    cell.setCellValue(value.toString());
                } else {
                    cell.setCellValue("");
                }
            }
        }

        // Convert workbook to byte array
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        workbook.write(byteArrayOutputStream);
        workbook.close();

        // Convert byte array to Base64-encoded string
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        String base64String = Base64.getEncoder().encodeToString(byteArray);

        return ResponseEntity.ok(base64String);
    }

    @PostMapping("/insert")
    public void insertEntity(@RequestBody EntityClass entity) {
        // Perform validation and other business logic as needed
        yourEntityService.insertEntity(entity);
    }
}