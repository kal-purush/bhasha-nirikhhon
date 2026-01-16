package com.sqlu.tools.excel.poi;

import com.sqlu.tools.excel.IExcelOperation;
import com.sqlu.tools.excel.exception.ExcelException;
import com.sqlu.tools.excel.pojo.*;
import com.sqlu.tools.excel.pojo.Font;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.util.Assert;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Poi操作
 * @author: stonelu
 * @create: 2019-12-21 10:03
 **/
public class PoiUtil implements IExcelOperation {
    private PoiUtil() {}
    public static final PoiUtil INSTANCE = new PoiUtil();

    public XSSFWorkbook createXssfWorkbook() {
        return new XSSFWorkbook();
    }

    public XSSFSheet createSheet(XSSFWorkbook workbook, int sheetIdx) {
        return this.createSheet(workbook, sheetIdx, null);
    }

    public XSSFSheet createSheet(XSSFWorkbook workbook, String sheetName) {
        return this.createSheet(workbook, sheetName, null);
    }

    /**
     * 创建sheet
     * @param workbook excel对象
     * @param sheetIdx sheet索引
     * @param sheetTabColor sheet标签颜色
     * @return
     */
    public XSSFSheet createSheet(XSSFWorkbook workbook, int sheetIdx, Color sheetTabColor) {
        return this.createSheet(workbook, getSheetName(sheetIdx), sheetTabColor);
    }

    public XSSFSheet createSheet(XSSFWorkbook workbook, String sheetName, Color sheetTabColor) {
        XSSFSheet sheet = workbook.createSheet(sheetName);
        if (nonNull(sheetTabColor)) {
            sheet.setTabColor(new XSSFColor(sheetTabColor));
        }

        return sheet;
    }

    /**
     * 获取sheet名称
     * @param sheetIdx sheet索引
     * @return
     */
    private String getSheetName(int sheetIdx) {
        return "Sheet" + sheetIdx;
    }



    // -------------- 内容渲染 begin ------------------

    /**
     * 渲染表头
     * @param workbook
     * @param sheet
     * @param header
     * @return
     */
    public void drawHeader(XSSFWorkbook workbook, XSSFSheet sheet, Header header) {
        List<Header.HeaderRowBuilder> headerRows = header.getHeaderRows();
        if (CollectionUtils.isEmpty(headerRows)) {
            return;
        }

        // 单元格样式
        XSSFCellStyle cellStyle = createCellStyle(workbook, header.getFont(), header.getBorder(), header.getForeground(), header.getAlign());

        for (int rowIdx = 0; rowIdx < headerRows.size(); rowIdx++) {
            Header.HeaderRowBuilder rowBuilder = headerRows.get(rowIdx);
            List<Cell> cells = rowBuilder.build();
            drawRow(sheet, cellStyle, rowIdx, cells);
        }

        // 列id-宽度
        Map<Integer, Integer> colIdxWidthMap = header.getColIdxWidthMap();
        if (MapUtils.isNotEmpty(colIdxWidthMap)) {
            for (Map.Entry<Integer, Integer> entry : colIdxWidthMap.entrySet()) {
                Integer colIdx = entry.getKey();
                Integer width = entry.getValue();
                setColWidth(sheet, colIdx, width);
            }
        }
    }

    /**
     * 渲染表头行
     * @param sheet sheet
     * @param cellStyle 单元格样式
     * @param currRowIdx 当前行索引
     * @param rowCells 当前行所有单元格
     */
    private void drawRow(XSSFSheet sheet, XSSFCellStyle cellStyle, int currRowIdx, List<Cell> rowCells) {
        // 创建当前行
        XSSFRow row = createRow(sheet, currRowIdx);

        // 如果当前行内容为空，则显示空行
        if (CollectionUtils.isEmpty(rowCells)) {
            return;
        }

        int colIdx = 0;

        for (int i = 0; i < rowCells.size(); i++) {
            Cell cell = rowCells.get(i);
            if (isNull(cell)) {
                colIdx++;
                continue;
            }

            XSSFCellStyle currCellStyle = getCellStyle(cellStyle, cell);

            // 跳过几个水平单元格
            Integer skipCellCount = cell.getSkipCellCount();
            if (nonNull(skipCellCount)) {
                if (skipCellCount < 0) {
                    throw new ExcelException("跳过的单元格数量不允许小于0");
                }

                if (skipCellCount > 0) {
                    // 需要跳过(水平)单元格数量
                    colIdx += skipCellCount;
                }
            }

            int incColIdx = 1;
            if (cell instanceof MergeCell) {
                // 如果是需要合并单元格的，则需要特殊处理
                MergeCell mergeCell = (MergeCell) cell;

                // 获取水平、垂直 合并单元格数量
                Integer mergeRowCount = mergeCell.getMergeRowCount();
                Integer mergeColCount = mergeCell.getMergeColCount();

                // 合并单元格并设置内容
                mergeAndSetCellValue(sheet, currRowIdx, mergeRowCount, colIdx, mergeColCount, mergeCell.getContent(), currCellStyle);

                incColIdx = mergeColCount;
            } else {
                createAndSetCellValue(row, colIdx, cell.getContent(), currCellStyle);
            }

            // 列号
            colIdx += incColIdx;
        }
    }

    /**
     * 获取单元格样式
     * @param oriStyle
     * @param cell
     * @return
     */
    private XSSFCellStyle getCellStyle(XSSFCellStyle oriStyle, Cell cell) {
        XSSFCellStyle currCellStyle = oriStyle;

        // 前景色
        Foreground foreground = cell.getForeground();
        if (nonNull(foreground)) {
            if (nonNull(foreground.getColor())) {
                currCellStyle = (XSSFCellStyle) oriStyle.clone();
                setCellForegroundColor(currCellStyle, foreground);
            }
        }

        // 自动换行
        if (isTrue(cell.getIsAutoWrapText())) {
            currCellStyle.setWrapText(true);
        }

        // 对齐
        if (nonNull(cell.getAlign())) {
            setAlign(currCellStyle, cell.getAlign());
        }

        return currCellStyle;
    }

    /**
     * 渲染表格内容
     * @param workbook 表格对象
     * @param sheet sheet对象
     * @param beginRowIdx 内容起始行号
     * @param content 内容对象
     */
    public void drawContent(XSSFWorkbook workbook, XSSFSheet sheet, int beginRowIdx, Content content) {
        Assert.notNull(workbook);
        Assert.notNull(content);

        // 单元格样式
        XSSFCellStyle cellStyle = createCellStyle(workbook, content.getFont(), content.getBorder(), content.getForeground(), content.getAlign());

        // 每行内容进行渲染
        int rowIdx = beginRowIdx;
        for (Content.ContentRowBuilder contentRow : content.getContentRows()) {
            List<Cell> cells = contentRow.build();
            drawRow(sheet, cellStyle, rowIdx++, cells);
        }

        // 设置行高
        if (MapUtils.isNotEmpty(content.getRowIdxHeightMap())) {
            for (Map.Entry<Integer, Integer> entry : content.getRowIdxHeightMap().entrySet()) {
                Integer heightRowIdx = entry.getKey();
                Integer rowHeight = entry.getValue();
                setRowHeight(sheet, heightRowIdx, rowHeight);
            }
        }
    }



    // -------------- 内容渲染 end ------------------

    // -------------- 样式 begin ------------------
    /**
     * 创建字体
     * @param workbook excel对象
     * @param font 字体POJO
     * @return
     */
    private XSSFFont createFont(XSSFWorkbook workbook, Font font) {
        if (isNull(font)) {
            return null;
        }

        XSSFFont xssfFont = workbook.createFont();

        // 字体大小
        Double size = font.getSize();
        if (nonNull(size)) {
            xssfFont.setFontHeight(size);
        }

        // 是否加粗
        Boolean isBold = font.getIsBold();
        if (nonNull(isBold)) {
            xssfFont.setBold(isBold);
        }

        String fontName = font.getFontName();
        if (StringUtils.isNotBlank(fontName)) {
            xssfFont.setFontName(fontName);
        }

        return xssfFont;
    }

    /**
     * 设置单元格边框样式
     * @param style
     * @param border
     */
    private void setCellBorder(XSSFCellStyle style, Border border) {
        if (isNull(border)) {
            return;
        }

        // 暂时默认所有边框都是细边框，后面可以按需拓展
        BorderStyle borderStyle = BorderStyle.THIN;

        if (isTrue(border.getContainTop())) {
            style.setBorderTop(borderStyle);
        }

        if (isTrue(border.getContainBottom())) {
            style.setBorderBottom(borderStyle);
        }

        if (isTrue(border.getContainLeft())) {
            style.setBorderLeft(borderStyle);
        }

        if (isTrue(border.getContainRight())) {
            style.setBorderRight(borderStyle);
        }
    }

    /**
     * 设置前景色
     * @param style 单元格样式
     * @param foreground 前景色对象
     */
    private void setCellForegroundColor(XSSFCellStyle style, Foreground foreground) {
        if (isNull(foreground)) {
            return;
        }

        Color color = foreground.getColor();
        if (nonNull(color)) {
            // 需要设置前景色为实色
            style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            style.setFillForegroundColor(new XSSFColor(color));
        }
    }

    /**
     * 设置对齐方式
     * @param cellStyle 单元格样式
     * @param align 对齐方式
     */
    private void setAlign(XSSFCellStyle cellStyle, Align align) {
        if (isNull(align)) {
            return;
        }

        // 水平对齐
        HorizontalAlignment hAlign = null;
        if (isTrue(align.getIsLeft())) {
            hAlign = HorizontalAlignment.LEFT;
        }

        if (isTrue(align.getIsHorizontalCenter())) {
            hAlign = HorizontalAlignment.CENTER;
        }

        if (isTrue(align.getIsRight())) {
            hAlign = HorizontalAlignment.RIGHT;
        }

        cellStyle.setAlignment(hAlign);


        // 垂直对齐
        VerticalAlignment vAlign = null;
        if (isTrue(align.getIsTop())) {
            vAlign = VerticalAlignment.TOP;
        }

        if (isTrue(align.getIsVerticalCenter())) {
            vAlign = VerticalAlignment.CENTER;
        }

        if (isTrue(align.getIsBottom())) {
            vAlign = VerticalAlignment.BOTTOM;
        }

        cellStyle.setVerticalAlignment(vAlign);
    }

    /**
     * 创建单元格样式
     *
     * @param book excel实体
     * @return
     */
    private XSSFCellStyle createCellStyle(XSSFWorkbook book) {
        return book.createCellStyle();
    }

    /**
     * 创建单元格样式
     * @param workbook excel对象
     * @param font 字体
     * @param border 边框
     * @param foreground 前景色
     * @param align 对齐
     * @return
     */
    private XSSFCellStyle createCellStyle(XSSFWorkbook workbook, Font font, Border border, Foreground foreground, Align align) {
        XSSFCellStyle cellStyle = createCellStyle(workbook);

        // 设置 字体
        cellStyle.setFont(createFont(workbook, font));

        // 设置边框样式
        setCellBorder(cellStyle, border);

        // 前景色
        setCellForegroundColor(cellStyle, foreground);

        // 对齐
        setAlign(cellStyle, align);

        return cellStyle;
    }
    // -------------- 样式 end ------------------


    // -------------- 单元格 begin ------------------
    /**
     * 创建并设置单元格内容、样式
     *
     * @param row    行对象
     * @param colIdx 列索引下标
     * @param value  值
     * @param style  单元格样式
     */
    private XSSFCell createAndSetCellValue(XSSFRow row, int colIdx, Object value, XSSFCellStyle style) {
        XSSFCell cell = createAndSetCellValue(row, colIdx, value);
        cell.setCellStyle(style);
        return cell;
    }

    /**
     * 创建并设置单元格内容
     *
     * @param row
     * @param colIdx
     * @param value
     */
    private XSSFCell createAndSetCellValue(XSSFRow row, int colIdx, Object value) {
        XSSFCell cell = row.getCell(colIdx);
        if (isNull(cell)) {
            cell = row.createCell(colIdx);
        }
        cell.setCellValue(stringOf(value));
        return cell;
    }

    /**
     * 创建并设置单元格内容
     *
     * @param row
     * @param colIdx
     * @param value
     */
    private XSSFCell createAndSetCellValue(XSSFRow row, int colIdx, Object value, Boolean isAutoWrapText) {
        XSSFCell cell = row.getCell(colIdx);
        if (isNull(cell)) {
            cell = row.createCell(colIdx);
        }

        if (isTrue(isAutoWrapText)) {
            cell.setCellValue(new XSSFRichTextString(stringOf(value)));
        } else {
            cell.setCellValue(stringOf(value));
        }

        return cell;
    }

    /**
     * 设置列宽度
     * @param sheet sheet
     * @param colIdx 列索引
     * @param width 列宽度
     */
    private void setColWidth(XSSFSheet sheet, int colIdx, int width) {
        sheet.setColumnWidth(colIdx, width * 255);
    }

    /**
     * 设置行高度
     * @param sheet sheet
     * @param rowIdx 行索引
     * @param height 行高度
     */
    private void setRowHeight(XSSFSheet sheet, int rowIdx, int height) {
        XSSFRow row = createRow(sheet, rowIdx);
        row.setHeight((short) (height * 255));
    }

    // -------------- 单元格 begin ------------------



    // -------------- 单元格操作 begin ------------------
    /**
     * 创建行
     * @param sheet
     * @param rowIdx
     * @return
     */
    private XSSFRow createRow(XSSFSheet sheet, int rowIdx) {
        XSSFRow row = sheet.getRow(rowIdx);
        if (isNull(row)) {
            row = sheet.createRow(rowIdx);
        }

        return row;
    }

    /**
     * 合并单元格
     * @param sheet sheet
     * @param beginRowIdx 起始行号
     * @param rowCount 需合并行数
     * @param beginColIdx 起始列号
     * @param colCount 需合并列数
     */
    private void mergeCells(XSSFSheet sheet, int beginRowIdx, int rowCount, int beginColIdx, int colCount) {
        // 最后一个行号
        int lastRowIdx = beginRowIdx + rowCount -1;
        // 最后一个列号
        int lastColIdx = beginColIdx + colCount - 1;

        CellRangeAddress mergeRegion = new CellRangeAddress(beginRowIdx, lastRowIdx, beginColIdx, lastColIdx);
        sheet.addMergedRegion(mergeRegion);
    }

    /**
     * 合并单元格并设置内容、样式
     * @param sheet sheet
     * @param beginRowIdx 起始行号
     * @param rowCount 需合并行数
     * @param beginColIdx 起始列号
     * @param colCount 需合并列数
     * @param value 内容
     * @param cellStyle 单元格样式
     * @return
     */
    private XSSFCell mergeAndSetCellValue(XSSFSheet sheet, int beginRowIdx, int rowCount, int beginColIdx, int colCount, Object value, XSSFCellStyle cellStyle) {
        XSSFCell retCell = null;

        for (int i = 0; i < rowCount; i++) {
            int rowIdx = beginRowIdx + i;
            XSSFRow row = createRow(sheet, rowIdx);

            for (int j = 0; j < colCount; j++) {
                int colIdx = beginColIdx + j;

                if (i == 0 && j == 0) {
                    retCell = createAndSetCellValue(row, colIdx, stringOf(value), cellStyle);
                } else {
                    // 其他内容需要置空内容，为了单元格样式相同即可
                    createAndSetCellValue(row, colIdx, "", cellStyle);
                }
            }
        }

        // 合并单元格
        mergeCells(sheet, beginRowIdx, rowCount, beginColIdx, colCount);

        return retCell;
    }

    /**
     * 设置列宽度
     *
     * @param sheet            sheet
     * @param colIdx           列索引下标
     * @param widthOfCharCount 列宽度（字符个数）
     */
    private void setColumnWidth(XSSFSheet sheet, int colIdx, int widthOfCharCount) {
        sheet.setColumnWidth(colIdx, widthOfCharCount * 256);
    }
    // -------------- 单元格操作 end ------------------


    // -------------- excel输出 begin ------------------
    /**
     * 将excel写入到Response中
     * @param response response
     * @param exportFileName 导出表格文件名称
     * @param workbook 表格对象
     */
    public void writeExcel2Response(HttpServletResponse response, String exportFileName, XSSFWorkbook workbook) throws IOException {
        Assert.notNull(response);
        Assert.notNull(workbook);
        Assert.hasText(exportFileName);

        // response头设置为excel内容
        response.setContentType("application/vnd.ms-excel;charset=utf-8");
        response.setHeader("Content-Disposition", "attachment;filename=" + new String(exportFileName.getBytes(), "iso-8859-1"));

        ServletOutputStream outputStream = response.getOutputStream();
        try {
            workbook.write(outputStream);
            outputStream.flush();
            outputStream.close();
        } catch (Exception e) {
            throw new ExcelException("输出excel到流中失败", e);
        }
    }

    /**
     * 将excel写入到文件中
     * @param exportFileFullPath 文件全路径
     * @param workbook excel对象
     */
    public void writeExcel2File(String exportFileFullPath, XSSFWorkbook workbook) {
        Assert.notNull(workbook);
        Assert.hasText(exportFileFullPath);

        File file = FileUtils.getFile(exportFileFullPath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new ExcelException("输出excel到文件前，创建文件失败：" + exportFileFullPath, e);
            }
        }

        try(FileOutputStream outputStream = new FileOutputStream(file)) {
            workbook.write(outputStream);
        } catch (IOException e) {
            throw new ExcelException("输出excel到文件失败", e);
        }
    }
    // -------------- excel输出 end ------------------



    private static boolean isNull(Object o) {return null == o;}
    private static boolean nonNull(Object o) {return !isNull(o);}
    private static boolean isTrue(Boolean b) {return  nonNull(b) && b;}
    private static boolean isFalse(Boolean b) {return nonNull(b) && !b;}

    private static String stringOf(Object o) {return String.valueOf(o);}

}