package com.springtest.ws.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

@Entity(name="quotations")
public class QuotationEntity implements Serializable {
    private static final long serialVersionUID = 4556236915178808724L;

    @Id
    public int quotCode;

    @Column(nullable = false)
    public int quotNo;

    public int quotClntCode;
    public int quotBrnCode;
    public int quotCurCode;
    public String quotCurSymbol;
    public Date quotCoverFrom;
    public Date quotCoverTo;
    public BigDecimal quotTotPropertyVal;
    public Date quotDate;
    public String quotStatus;
    public Date quotExpiryDate;
    public BigDecimal quotPremium;
    public BigDecimal quotCommAmt;
    public String quotAuthorisedBy;
    public Date quotAuthorisedDt;
    public String quotConfirmed;
    public String quotConfirmedBy;
    public Date quotConfirmedDt;
    //    public String quotReady;
    public String quotReadyBy;
    public Date quotReadyDate;
    public String quotRevised;
    public String quotPreparedBy;
    public String quotClntType;
    public Date quotPreparedDt;
    public String quotCancelReason;
    public String quotFreqOfPayment;

    public int getQuotCode() {
        return quotCode;
    }

    public void setQuotCode(int quotCode) {
        this.quotCode = quotCode;
    }

    public int getQuotNo() {
        return quotNo;
    }

    public void setQuotNo(int quotNo) {
        this.quotNo = quotNo;
    }

    public int getQuotClntCode() {
        return quotClntCode;
    }

    public void setQuotClntCode(int quotClntCode) {
        this.quotClntCode = quotClntCode;
    }

    public int getQuotBrnCode() {
        return quotBrnCode;
    }

    public void setQuotBrnCode(int quotBrnCode) {
        this.quotBrnCode = quotBrnCode;
    }

    public int getQuotCurCode() {
        return quotCurCode;
    }

    public void setQuotCurCode(int quotCurCode) {
        this.quotCurCode = quotCurCode;
    }

    public String getQuotCurSymbol() {
        return quotCurSymbol;
    }

    public void setQuotCurSymbol(String quotCurSymbol) {
        this.quotCurSymbol = quotCurSymbol;
    }

    public Date getQuotCoverFrom() {
        return quotCoverFrom;
    }

    public void setQuotCoverFrom(Date quotCoverFrom) {
        this.quotCoverFrom = quotCoverFrom;
    }

    public Date getQuotCoverTo() {
        return quotCoverTo;
    }

    public void setQuotCoverTo(Date quotCoverTo) {
        this.quotCoverTo = quotCoverTo;
    }

    public BigDecimal getQuotTotPropertyVal() {
        return quotTotPropertyVal;
    }

    public void setQuotTotPropertyVal(BigDecimal quotTotPropertyVal) {
        this.quotTotPropertyVal = quotTotPropertyVal;
    }

    public Date getQuotDate() {
        return quotDate;
    }

    public void setQuotDate(Date quotDate) {
        this.quotDate = quotDate;
    }

    public String getQuotStatus() {
        return quotStatus;
    }

    public void setQuotStatus(String quotStatus) {
        this.quotStatus = quotStatus;
    }

    public Date getQuotExpiryDate() {
        return quotExpiryDate;
    }

    public void setQuotExpiryDate(Date quotExpiryDate) {
        this.quotExpiryDate = quotExpiryDate;
    }

    public BigDecimal getQuotPremium() {
        return quotPremium;
    }

    public void setQuotPremium(BigDecimal quotPremium) {
        this.quotPremium = quotPremium;
    }

    public BigDecimal getQuotCommAmt() {
        return quotCommAmt;
    }

    public void setQuotCommAmt(BigDecimal quotCommAmt) {
        this.quotCommAmt = quotCommAmt;
    }

    public String getQuotAuthorisedBy() {
        return quotAuthorisedBy;
    }

    public void setQuotAuthorisedBy(String quotAuthorisedBy) {
        this.quotAuthorisedBy = quotAuthorisedBy;
    }

    public Date getQuotAuthorisedDt() {
        return quotAuthorisedDt;
    }

    public void setQuotAuthorisedDt(Date quotAuthorisedDt) {
        this.quotAuthorisedDt = quotAuthorisedDt;
    }

    public String getQuotConfirmed() {
        return quotConfirmed;
    }

    public void setQuotConfirmed(String quotConfirmed) {
        this.quotConfirmed = quotConfirmed;
    }

    public String getQuotConfirmedBy() {
        return quotConfirmedBy;
    }

    public void setQuotConfirmedBy(String quotConfirmedBy) {
        this.quotConfirmedBy = quotConfirmedBy;
    }

    public Date getQuotConfirmedDt() {
        return quotConfirmedDt;
    }

    public void setQuotConfirmedDt(Date quotConfirmedDt) {
        this.quotConfirmedDt = quotConfirmedDt;
    }

    public String getQuotReadyBy() {
        return quotReadyBy;
    }

    public void setQuotReadyBy(String quotReadyBy) {
        this.quotReadyBy = quotReadyBy;
    }

    public Date getQuotReadyDate() {
        return quotReadyDate;
    }

    public void setQuotReadyDate(Date quotReadyDate) {
        this.quotReadyDate = quotReadyDate;
    }

    public String getQuotRevised() {
        return quotRevised;
    }

    public void setQuotRevised(String quotRevised) {
        this.quotRevised = quotRevised;
    }

    public String getQuotPreparedBy() {
        return quotPreparedBy;
    }

    public void setQuotPreparedBy(String quotPreparedBy) {
        this.quotPreparedBy = quotPreparedBy;
    }

    public String getQuotClntType() {
        return quotClntType;
    }

    public void setQuotClntType(String quotClntType) {
        this.quotClntType = quotClntType;
    }

    public Date getQuotPreparedDt() {
        return quotPreparedDt;
    }

    public void setQuotPreparedDt(Date quotPreparedDt) {
        this.quotPreparedDt = quotPreparedDt;
    }

    public String getQuotCancelReason() {
        return quotCancelReason;
    }

    public void setQuotCancelReason(String quotCancelReason) {
        this.quotCancelReason = quotCancelReason;
    }

    public String getQuotFreqOfPayment() {
        return quotFreqOfPayment;
    }

    public void setQuotFreqOfPayment(String quotFreqOfPayment) {
        this.quotFreqOfPayment = quotFreqOfPayment;
    }
}