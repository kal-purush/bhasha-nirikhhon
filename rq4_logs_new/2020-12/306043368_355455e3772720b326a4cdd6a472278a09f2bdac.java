package org.example.common.util;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.builder.ExcelWriterBuilder;
import com.alibaba.excel.write.builder.ExcelWriterSheetBuilder;
import com.alibaba.excel.write.handler.AbstractRowWriteHandler;
import com.alibaba.excel.write.handler.SheetWriteHandler;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.excel.write.metadata.holder.WriteSheetHolder;
import com.alibaba.excel.write.metadata.holder.WriteTableHolder;
import com.alibaba.excel.write.metadata.holder.WriteWorkbookHolder;

import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.DataValidationHelper;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFDataValidation;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;

/**
 * excel工具类
 *
 * <pre>
 * easyexcel官方文档: https://www.yuque.com/easyexcel/doc/write
 * </pre>
 *
 * @author walle
 * @version V1.0
 * @since 2020-12-12 22:04
 */
public class ExcelUtil {

    /**
     * excel下面选择Handler
     *
     * @author walle
     * @version V1.0
     * @since 2020-12-12 22:55
     */
    public static class SelectorSheetWriteHandler implements SheetWriteHandler {

        private Map<Integer, String[]> columnSelectorMap;

        /**
         * 下拉选择框
         * <p>
         * key: 对应excelTemplate的@ExcelProperty注解的index值
         * <p>
         * value: 该下拉选择框的下拉内容
         *
         * @param columnSelectorMap 下拉选择框数据
         */
        public SelectorSheetWriteHandler(Map<Integer, String[]> columnSelectorMap) {
            this.columnSelectorMap = columnSelectorMap;
        }

        @Override
        public void beforeSheetCreate(WriteWorkbookHolder writeWorkbookHolder, WriteSheetHolder writeSheetHolder) {

        }

        @Override
        public void afterSheetCreate(WriteWorkbookHolder writeWorkbookHolder, WriteSheetHolder writeSheetHolder) {
            Sheet sheet = writeSheetHolder.getSheet();

            //开始设置下拉框
            DataValidationHelper helper = sheet.getDataValidationHelper();
            for (Map.Entry<Integer, String[]> entry : columnSelectorMap.entrySet()) {
                /* 起始行、终止行、起始列、终止列 */
                int maxExcelRow = 1048575;
                CellRangeAddressList addressList = new CellRangeAddressList(1, maxExcelRow, entry.getKey(),
                    entry.getKey());
                /* 设置下拉框数据 */
                DataValidationConstraint constraint = helper.createExplicitListConstraint(entry.getValue());
                DataValidation dataValidation = helper.createValidation(constraint, addressList);
                /* 处理Excel兼容性问题 */
                if (dataValidation instanceof XSSFDataValidation) {
                    dataValidation.setSuppressDropDownArrow(true);
                    dataValidation.setShowErrorBox(true);
                } else {
                    dataValidation.setSuppressDropDownArrow(false);
                }
                sheet.addValidationData(dataValidation);
            }

        }
    }

    /**
     * excel批注对象
     */
    public static class ExcelCellComment {
        /**
         * 行号,从0开始
         */
        private int rowNum;
        /**
         * 单元格,从0开始
         */
        private int cellNum;
        /**
         * 备注
         */
        private String comment;

        public int getRowNum() {
            return rowNum;
        }

        public void setRowNum(int rowNum) {
            this.rowNum = rowNum;
        }

        public int getCellNum() {
            return cellNum;
        }

        public void setCellNum(int cellNum) {
            this.cellNum = cellNum;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
    }

    /**
     * @author walle
     * @version V1.0
     * @since 2020-12-12 23:09
     */
    public static class CommentWriteHandler extends AbstractRowWriteHandler {

        private List<ExcelCellComment> remarkList;

        public CommentWriteHandler(List<ExcelCellComment> remarkList) {
            this.remarkList = remarkList;
        }

        @Override
        public void afterRowDispose(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, Row row,
            Integer relativeRowIndex, Boolean isHead) {
            if (isHead) {
                Sheet sheet = writeSheetHolder.getSheet();
                Drawing<?> drawingPatriarch = sheet.createDrawingPatriarch();

                /*
                 * Creates a new client anchor and sets the top-left and bottom-right
                 * coordinates of the anchor by cell references and offsets.
                 * Sets the type to {@link org.apache.poi.ss.usermodel.ClientAnchor.AnchorType#MOVE_AND_RESIZE}.
                 *
                 * @param dx1  the x coordinate within the first cell.
                 * @param dy1  the y coordinate within the first cell.
                 * @param dx2  the x coordinate within the second cell.
                 * @param dy2  the y coordinate within the second cell.
                 * @param col1 the column (0 based) of the first cell.
                 * @param row1 the row (0 based) of the first cell.
                 * @param col2 the column (0 based) of the second cell.
                 * @param row2 the row (0 based) of the second cell.
                 */

                for (ExcelCellComment excelCellComment : remarkList) {
                    XSSFClientAnchor anchor = new XSSFClientAnchor(0, 0, 0, 0, (short) excelCellComment.getCellNum(), 0,
                        (short) excelCellComment.getCellNum() + 1, 0);
                    Comment comment = drawingPatriarch.createCellComment(anchor);
                    // 输入批注信息
                    comment.setString(new XSSFRichTextString(excelCellComment.getComment()));

                    // 将批注添加到单元格对象中
                    sheet.getRow(excelCellComment.getRowNum()).getCell(excelCellComment.getCellNum())
                        .setCellComment(comment);
                }
            }
        }
    }

    /**
     * 设置头部信息
     *
     * @param response HttpServletResponse
     * @param fileName 文件名称
     */
    public static void setResponseHeader(HttpServletResponse response, String fileName) throws Exception {
        String encodeName = URLEncoder.encode(fileName, "UTF-8").replaceAll("\\+", "%20");

        response.setContentType("application/vnd.ms-excel");
        response.setCharacterEncoding("utf-8");
        response.addHeader("Access-Control-Expose-Headers", "Content-Disposition");
        response.setHeader("Content-Disposition", "attachment;filename*=utf-8''" + encodeName + ".xlsx");
    }

    /**
     * 创建excel write
     *
     * @param outputStream 输出流
     * @return ExcelWriter
     */
    public static ExcelWriter createExcelWriter(ServletOutputStream outputStream, Class<?> excelTemplateClass) {
        ExcelWriterBuilder excelWriterBuilder = new ExcelWriterBuilder();
        excelWriterBuilder.autoCloseStream(true);
        excelWriterBuilder.file(outputStream);
        excelWriterBuilder.head(excelTemplateClass);

        return excelWriterBuilder.build();
    }

    /**
     * 创建writeSheet
     *
     * @return WriteSheet
     */
    public static WriteSheet createWriteSheet(String sheetName, SelectorSheetWriteHandler selectorSheetWriteHandler,
        CommentWriteHandler commentWriteHandler, Set<String> excludeColumns) {

        ExcelWriterSheetBuilder writerSheetBuilder = EasyExcel.writerSheet(sheetName);

        // 设置下拉选择数据
        if (Objects.nonNull(selectorSheetWriteHandler)) {
            writerSheetBuilder.registerWriteHandler(selectorSheetWriteHandler);
        }

        // 设置备注
        if (Objects.nonNull(commentWriteHandler)) {
            writerSheetBuilder.registerWriteHandler(commentWriteHandler);
        }

        // 设置忽略字段属性
        if (Objects.nonNull(excludeColumns) && !excludeColumns.isEmpty()) {
            writerSheetBuilder.excludeColumnFiledNames(excludeColumns);
        }

        return writerSheetBuilder.build();
    }
}