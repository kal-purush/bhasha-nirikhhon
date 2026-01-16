package com.example.libraryapp;

public class AllrentalinfoData {
    String bookName;
    String startDate;
    String endDate;

    public String getBookName() {
        return bookName;
    }

    public String getStartDate() {
        return startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public AllrentalinfoData(String bookName, String startDate, String endDate) {
        this.bookName = bookName;
        this.startDate = startDate;
        this.endDate = endDate;
    }
}