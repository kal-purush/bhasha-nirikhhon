package org.example.common.util.excel;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.builder.ExcelWriterBuilder;
import com.alibaba.excel.write.builder.ExcelWriterSheetBuilder;
import com.alibaba.excel.write.metadata.WriteSheet;

import java.net.URLEncoder;
import java.util.Objects;
import java.util.Set;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.example.common.util.excel.handler.CommentWriteHandler;
import org.example.common.util.excel.handler.SelectorSheetWriteHandler;

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

        ExcelWriterBuilder write = EasyExcel.write();
        write.autoCloseStream(false);

        ExcelWriterSheetBuilder writerSheetBuilder = write.sheet(sheetName);

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