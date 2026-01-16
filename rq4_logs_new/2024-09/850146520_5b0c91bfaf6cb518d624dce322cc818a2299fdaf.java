package com.example.samplebatch.batch;

import com.example.samplebatch.entity.BeforeEntity;
import lombok.RequiredArgsConstructor;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.FileOutputStream;
import java.io.IOException;

@RequiredArgsConstructor
public class ExcelRowWriter implements ItemStreamWriter<BeforeEntity> {

    private final String filePath;
    private Workbook workbook;
    private Sheet sheet;
    private int currentRowNumber;
    private boolean isClosed;

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        // 코드 동작 시 단 한 번만 실행되는 메서드로 엑셀 파일 생성이나 값을 초기화하는 로직 기입
        workbook = new XSSFWorkbook();
        sheet = workbook.createSheet("Sheet1");
    }

    @Override
    public void write(Chunk<? extends BeforeEntity> chunk) {
        for (BeforeEntity entity : chunk) {
            Row row = sheet.createRow(currentRowNumber++);
            row.createCell(0).setCellValue(entity.getUsername());
        }
    }

    @Override
    public void close() throws ItemStreamException {

        if (isClosed) {
            return;
        }

        try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
            workbook.write(fileOut);
        } catch (IOException e) {
            throw new ItemStreamException(e);
        } finally {
            try {
                workbook.close();
            } catch (IOException e) {
                throw new ItemStreamException(e);
            } finally {
                isClosed = true;
            }
        }
    }
}