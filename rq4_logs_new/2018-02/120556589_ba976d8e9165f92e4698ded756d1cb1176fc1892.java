package com.zwk.tool.exceller.util.object;

import com.zwk.tool.exceller.annotation.ExcelColumn;
import com.zwk.tool.exceller.annotation.ExcelSheet;
import com.zwk.tool.exceller.annotation.ExcelTable;

import java.util.Date;
import java.util.Objects;

@ExcelSheet(name = "Sheet2")
@ExcelTable("Table1")
public class Example5 {

    @ExcelColumn("aa")
    private Date a;
    @ExcelColumn("bb")
    private int b;
    private String c;

    public Example5() {
        super();
    }

    public Example5(Date a, int b, String c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public Date getA() {
        return a;
    }

    public void setA(Date a) {
        this.a = a;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
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
                ", b=" + b +
                ", c='" + c + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Example5 example = (Example5) o;
        return b == example.b &&
                Objects.equals(a, example.a) &&
                Objects.equals(c, example.c);
    }

    @Override
    public int hashCode() {

        return Objects.hash(a, b, c);
    }
}