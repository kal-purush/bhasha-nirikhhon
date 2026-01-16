package org.example.backend.controllers.admin.banHang;


public class QrRequest {
    private String accountNo;
    private String accountName;
    private String acqId;
    private String addInfo;
    private double amount;

    // Getters và Setters
    public String getAccountNo() { return accountNo; }
    public void setAccountNo(String accountNo) { this.accountNo = accountNo; }

    public String getAccountName() { return accountName; }
    public void setAccountName(String accountName) { this.accountName = accountName; }

    public String getAcqId() { return acqId; }
    public void setAcqId(String acqId) { this.acqId = acqId; }

    public String getAddInfo() { return addInfo; }
    public void setAddInfo(String addInfo) { this.addInfo = addInfo; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
}