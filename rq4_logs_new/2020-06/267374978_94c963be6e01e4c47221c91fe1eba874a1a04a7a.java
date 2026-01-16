package com.example.taxio;

public class DriverData { //구조체를 촬용함
    private String driverName;
    private String driverInfo;
    private String driverPrice;
    private int driverPhoto;

    public String getDriverName() { //기사 이름
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getDriverInfo() { //기사 정보 3가지
        return driverInfo;
    }

    public void setDriverInfo(String driverInfo) {
        this.driverInfo = driverInfo;
    }

    public String getDirverPrice() { //기사 가격
        return driverPrice;
    }

    public void setDirverPrice(String driverPrice) {
        this.driverPrice = driverPrice;
    }

    public int getDriverPhoto() { //기사 사진
        return driverPhoto;
    }

    public void setDriverPhoto(int driverPhoto) {
        this.driverPhoto = driverPhoto;
    }
}