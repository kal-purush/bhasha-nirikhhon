package com.zwk.tool.exceller;

import com.zwk.tool.exceller.annotation.ExcelColumnName;
import com.zwk.tool.exceller.annotation.ExcelTableName;

@ExcelTableName("sample_table")
public class Example {

    @ExcelColumnName("aa")
    private String a;
    @ExcelColumnName("bb")
    private String b;
    private String c;

    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }

    @Override
    public String toString() {
        return "Example{" +
                "a='" + a + '\'' +
                ", b='" + b + '\'' +
                ", c='" + c + '\'' +
                '}';
    }
}