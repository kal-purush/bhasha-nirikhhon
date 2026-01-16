package org.example.common.domain.entity;

/**
 * Excel 单元格批注
 */
public class ExcelCellComment {
    /**
     * 行号
     */
    private int row;
    /**
     * 列号
     */
    private int col;
    /**
     * 批注
     */
    private String content;

    public int getRow() {
        return row;
    }

    public void setRow(int row) {
        this.row = row;
    }

    public int getCol() {
        return col;
    }

    public void setCol(int col) {
        this.col = col;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}