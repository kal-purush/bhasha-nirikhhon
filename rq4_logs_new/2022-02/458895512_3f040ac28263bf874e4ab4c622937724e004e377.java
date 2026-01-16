package ru.zhbert.docslinter.service;

public class TableLinesService {

    int dictMaxLen, firstColLen;
    String upLine, mediumLine, downLine;

    StringBuilder colFirstLine, colSecondLine;
    StringBuilder temp;

    public TableLinesService(int dictMaxLen, int firstColLen) {
        this.dictMaxLen = dictMaxLen;
        this.firstColLen = firstColLen;

        colFirstLine = new StringBuilder();
        for (int i = 0; i < firstColLen; i++) {
            colFirstLine.append("-");
        }

        colSecondLine = new StringBuilder();
        for (int i = 0; i < dictMaxLen; i++) {
            colSecondLine.append("-");
        }

        temp = new StringBuilder("┌-");
        temp.append(colFirstLine.toString())
                .append("-╷-")
                .append(colSecondLine.toString())
                .append("-╷-")
                .append(colSecondLine.toString())
                .append("-┐");
        this.upLine = temp.toString();

        temp = new StringBuilder("└-");
        temp.append(colFirstLine.toString())
                .append("-╵-")
                .append(colSecondLine.toString())
                .append("-╵-")
                .append(colSecondLine.toString())
                .append("-┘");
        this.downLine = temp.toString();

        temp = new StringBuilder("|-");
        temp.append(colFirstLine.toString())
                .append("-|-")
                .append(colSecondLine.toString())
                .append("-|-")
                .append(colSecondLine.toString())
                .append("-|");
        this.mediumLine = temp.toString();
    }

    public void printUpLine() {
        System.out.println(upLine);
    }

    public void printMediumLine() {
        System.out.println(mediumLine);
    }

    public void printDownLine() {
        System.out.println(downLine);
    }

    public int getDictMaxLen() {
        return dictMaxLen;
    }

    public void setDictMaxLen(int dictMaxLen) {
        this.dictMaxLen = dictMaxLen;
    }

    public int getFirstColLen() {
        return firstColLen;
    }

    public void setFirstColLen(int firstColLen) {
        this.firstColLen = firstColLen;
    }
}