package com.example.ffccloud;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class ProductModel {
    @SerializedName("It_Code")
    @Expose
    private double itCode;
    @SerializedName("It_Code_D")
    @Expose
    private String itCodeD;
    @SerializedName("It_Head")
    @Expose
    private String itHead;
    @SerializedName("Unit_Id")
    @Expose
    private double unitId;
    @SerializedName("Unit")
    @Expose
    private String unit;
    @SerializedName("Part_No")
    @Expose
    private String partNo;
    @SerializedName("Sale_Rate")
    @Expose
    private double saleRate;
    @SerializedName("id")
    @Expose
    private double id;
    @SerializedName("title")
    @Expose
    private String title;
    @SerializedName("DeptID")
    @Expose
    private double deptID;
    @SerializedName("IsService")
    @Expose
    private Boolean isService;
    @SerializedName("IsFinishedGood")
    @Expose
    private Boolean isFinishedGood;

    public ProductModel() {
    }

    public double getItCode() {
        return itCode;
    }

    public void setItCode(double itCode) {
        this.itCode = itCode;
    }

    public String getItCodeD() {
        return itCodeD;
    }

    public void setItCodeD(String itCodeD) {
        this.itCodeD = itCodeD;
    }

    public String getItHead() {
        return itHead;
    }

    public void setItHead(String itHead) {
        this.itHead = itHead;
    }

    public double getUnitId() {
        return unitId;
    }

    public void setUnitId(double unitId) {
        this.unitId = unitId;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getPartNo() {
        return partNo;
    }

    public void setPartNo(String partNo) {
        this.partNo = partNo;
    }

    public double getSaleRate() {
        return saleRate;
    }

    public void setSaleRate(double saleRate) {
        this.saleRate = saleRate;
    }

    public double getId() {
        return id;
    }

    public void setId(double id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getDeptID() {
        return deptID;
    }

    public void setDeptID(double deptID) {
        this.deptID = deptID;
    }

    public Boolean getService() {
        return isService;
    }

    public void setService(Boolean service) {
        isService = service;
    }

    public Boolean getFinishedGood() {
        return isFinishedGood;
    }

    public void setFinishedGood(Boolean finishedGood) {
        isFinishedGood = finishedGood;
    }
}