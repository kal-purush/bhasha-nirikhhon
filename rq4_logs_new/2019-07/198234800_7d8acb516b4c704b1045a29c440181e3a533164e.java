package ics.raghav.verticalhomes.All_Model_Classes;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Bricks_Form_Responce_data {

    @SerializedName("id")
    @Expose
    private String id;
    @SerializedName("admin_id")
    @Expose
    private String adminId;
    @SerializedName("date")
    @Expose
    private String date;
    @SerializedName("inward_time")
    @Expose
    private String inwardTime;
    @SerializedName("outward_time")
    @Expose
    private String outwardTime;
    @SerializedName("lorry_no")
    @Expose
    private String lorryNo;
    @SerializedName("chalan_no")
    @Expose
    private String chalanNo;
    @SerializedName("party_name")
    @Expose
    private String partyName;
    @SerializedName("quantity")
    @Expose
    private String quantity;
    @SerializedName("rate")
    @Expose
    private String rate;
    @SerializedName("amount")
    @Expose
    private String amount;
    @SerializedName("gst")
    @Expose
    private String gst;
    @SerializedName("gross_amount")
    @Expose
    private String grossAmount;
    @SerializedName("remark")
    @Expose
    private String remark;
    @SerializedName("attachment")
    @Expose
    private String attachment;
    @SerializedName("status")
    @Expose
    private String status;
    @SerializedName("verify")
    @Expose
    private String verify;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAdminId() {
        return adminId;
    }

    public void setAdminId(String adminId) {
        this.adminId = adminId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getInwardTime() {
        return inwardTime;
    }

    public void setInwardTime(String inwardTime) {
        this.inwardTime = inwardTime;
    }

    public String getOutwardTime() {
        return outwardTime;
    }

    public void setOutwardTime(String outwardTime) {
        this.outwardTime = outwardTime;
    }

    public String getLorryNo() {
        return lorryNo;
    }

    public void setLorryNo(String lorryNo) {
        this.lorryNo = lorryNo;
    }

    public String getChalanNo() {
        return chalanNo;
    }

    public void setChalanNo(String chalanNo) {
        this.chalanNo = chalanNo;
    }

    public String getPartyName() {
        return partyName;
    }

    public void setPartyName(String partyName) {
        this.partyName = partyName;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getGst() {
        return gst;
    }

    public void setGst(String gst) {
        this.gst = gst;
    }

    public String getGrossAmount() {
        return grossAmount;
    }

    public void setGrossAmount(String grossAmount) {
        this.grossAmount = grossAmount;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getAttachment() {
        return attachment;
    }

    public void setAttachment(String attachment) {
        this.attachment = attachment;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getVerify() {
        return verify;
    }

    public void setVerify(String verify) {
        this.verify = verify;
    }

}


