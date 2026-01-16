package com.github.classpick.room.util;

import com.github.classpick.room.exception.RoomException;
import com.github.classpick.room.exception.RoomExceptionCode;
import com.github.classpick.room.repository.RoomEntity;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

public class RoomExcelParserUtil {

    private static final int COL_PLACE_NAME = 0;
    private static final int COL_UNIT_NUMBER = 1;
    private static final int COL_CAPACITY = 2;
    private static final int COL_ALIAS = 3;
    private static final int COL_IMAGE = 4;

    public static List<RoomEntity> parse(InputStream is) {

        try (Workbook workbook = new XSSFWorkbook(is)) {
            Sheet sheet = workbook.getSheetAt(0);
            return IntStream.rangeClosed(1, sheet.getLastRowNum())
                    .mapToObj(sheet::getRow)
                    .filter(Objects::nonNull)
                    .filter(RoomExcelParserUtil::isRowNotEmpty)
                    .map(RoomExcelParserUtil::parseRow)
                    .toList();
        } catch (IOException e) {
            throw new RoomException(RoomExceptionCode.ROOM_EXCEL_PARSING_ERROR);
        }
    }

    private static boolean isRowNotEmpty(Row row) {

        return IntStream.rangeClosed(0, row.getLastCellNum())
                .mapToObj(row::getCell)
                .anyMatch(x -> x.getCellType() != CellType.BLANK &&
                        !(x.getCellType() == CellType.STRING && x.getStringCellValue().isBlank()));
    }

    private static RoomEntity parseRow(Row row) {

        String placeName = getCellValue(row.getCell(COL_PLACE_NAME)).orElseThrow(() -> new RoomException(
                "placeName의 값은 문자열(String) 타입이어야 합니다.",
                RoomExceptionCode.ROOM_EXCEL_INVALID_TYPE.getStatus()
        ));
        String unitNumber = getCellValue(row.getCell(COL_UNIT_NUMBER)).orElseThrow(() -> new RoomException(
                "unitNumber의 값은 문자열(String) 타입이어야 합니다.",
                RoomExceptionCode.ROOM_EXCEL_INVALID_TYPE.getStatus()
        ));
        Optional<Integer> capacity = getCellValueAsInt(row.getCell(COL_CAPACITY));
        Optional<String> alias = getCellValue(row.getCell(COL_ALIAS));
        Optional<String> image = getCellValue(row.getCell(COL_IMAGE));

        return RoomEntity.builder()
                .placeName(placeName)
                .unitNumber(unitNumber)
                .capacity(capacity.orElse(null))
                .alias(alias.map(String::trim).orElse(null))
                .image(image.map(String::trim).orElse(null))
                .build();
    }

    private static Optional<String> getCellValue(Cell cell) {

        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return Optional.empty();
        }

        return switch (cell.getCellType()) {
            case STRING -> {
                String value = cell.getStringCellValue().trim();
                yield value.isEmpty() ? Optional.empty() : Optional.of(value);
            }
            case NUMERIC -> {
                double numericValue = cell.getNumericCellValue();
                String value = (numericValue % 1 == 0)
                        ? String.valueOf((int) numericValue)
                        : String.valueOf(numericValue);
                yield Optional.of(value);
            }
            case BOOLEAN -> Optional.of(String.valueOf(cell.getBooleanCellValue()));
            default -> Optional.empty();
        };
    }


    private static Optional<Integer> getCellValueAsInt(Cell cell) {

        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return Optional.empty();
        }
        if (cell.getCellType() == CellType.NUMERIC) {
            return Optional.of((int) cell.getNumericCellValue());
        }
        if (cell.getCellType() == CellType.STRING) {
            try {
                return Optional.of(Integer.parseInt(cell.getStringCellValue().trim()));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}