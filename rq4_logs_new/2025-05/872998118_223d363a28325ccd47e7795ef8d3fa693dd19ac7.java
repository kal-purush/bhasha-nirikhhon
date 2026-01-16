package com.edw.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.Type;

import java.util.Date;

/**
 * <pre>
 *  com.edw.model.GenMdAccountEntity
 * </pre>
 *
 * @author Muhammad Edwin < edwin at redhat dot com >
 * 27 May 2025 1:10
 */
@Indexed
public class GenMdAccountEntity {
    private Long accountId;

    private String accountCode;

    private Long memberId;

    private String clientCode;

    private String accountType;

    private String checkDigit;

    private String accountName;

    private String accountPosition;

    private String correspondAcNo;

    private String blockingReason;

    private String accountStatus;

    private String createdBy;

    private Date createdOn;

    private String modifiedBy;

    private Date modifiedOn;

    private String checkedBy;

    private Date checkedOn;

    private String approvedBy;

    private Date approvedOn;

    private String accountBuyerSeller;

    @ProtoField(type = Type.INT64, number = 1, name = "ACCOUNT_ID")
    @Basic(sortable = true, searchable = true, projectable = true)
    public Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }

    @ProtoField(type = Type.STRING, number = 2, name = "ACCOUNT_CODE")
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getAccountCode() {
        return accountCode;
    }

    public void setAccountCode(String accountCode) {
        this.accountCode = accountCode;
    }

    @ProtoField(name = "MEMBER_ID", type = Type.INT64, number = 3)
    @Basic(sortable = true, searchable = true, projectable = true)
    public Long getMemberId() {
        return memberId;
    }

    public void setMemberId(Long memberId) {
        this.memberId = memberId;
    }

    @ProtoField(name = "CLIENT_CODE", type = Type.STRING, number = 4)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getClientCode() {
        return clientCode;
    }

    public void setClientCode(String clientCode) {
        this.clientCode = clientCode;
    }

    @ProtoField(name = "ACCOUNT_TYPE", type = Type.STRING, number = 5)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getAccountType() {
        return accountType;
    }

    public void setAccountType(String accountType) {
        this.accountType = accountType;
    }

    @ProtoField(name = "CHECK_DIGIT", type = Type.STRING, number = 6)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getCheckDigit() {
        return checkDigit;
    }

    public void setCheckDigit(String checkDigit) {
        this.checkDigit = checkDigit;
    }

    @ProtoField(name = "ACCOUNT_NAME", type = Type.STRING, number = 7)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    @ProtoField(name = "ACCOUNT_POSITION", type = Type.STRING, number = 8)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getAccountPosition() {
        return accountPosition;
    }

    public void setAccountPosition(String accountPosition) {
        this.accountPosition = accountPosition;
    }

    @ProtoField(name = "CORRESPOND_ACNO", type = Type.STRING, number = 9)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getCorrespondAcNo() {
        return correspondAcNo;
    }

    public void setCorrespondAcNo(String correspondAcNo) {
        this.correspondAcNo = correspondAcNo;
    }

    @ProtoField(name = "BLOCKING_REASON", type = Type.STRING, number = 10)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getBlockingReason() {
        return blockingReason;
    }

    public void setBlockingReason(String blockingReason) {
        this.blockingReason = blockingReason;
    }

    @ProtoField( name = "ACCOUNT_STATUS", type = Type.STRING, number = 11)
    @Basic(sortable = true, searchable = true, projectable = true)
    public String getAccountStatus() {
        return accountStatus;
    }


    public void setAccountStatus(String accountStatus) {
        this.accountStatus = accountStatus;
    }

    @ProtoField(name = "CREATED_BY", type = Type.STRING, number = 12)
    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @ProtoField(name = "CREATED_ON", type = Type.INT64, number = 13)
    public Date getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }

    @ProtoField(name = "MODIFIED_BY", type = Type.STRING, number = 14)
    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    @ProtoField(name = "MODIFIED_ON", type = Type.INT64, number = 15)
    public Date getModifiedOn() {
        return modifiedOn;
    }

    public void setModifiedOn(Date modifiedOn) {
        this.modifiedOn = modifiedOn;
    }

    @ProtoField(name = "CHECKED_BY", type = Type.STRING, number = 16)
    public String getCheckedBy() {
        return checkedBy;
    }

    public void setCheckedBy(String checkedBy) {
        this.checkedBy = checkedBy;
    }

    @ProtoField(type = Type.INT64, number = 17)
    public Date getCheckedOn() {
        return checkedOn;
    }

    public void setCheckedOn(Date checkedOn) {
        this.checkedOn = checkedOn;
    }

    @ProtoField(type = Type.STRING, number = 18)
    public String getApprovedBy() {
        return approvedBy;
    }


    public void setApprovedBy(String approvedBy) {
        this.approvedBy = approvedBy;
    }

    @ProtoField(type = Type.INT64, number = 19)
    public Date getApprovedOn() {
        return approvedOn;
    }

    public void setApprovedOn(Date approvedOn) {
        this.approvedOn = approvedOn;
    }

    @ProtoField(type = Type.STRING, number = 20)
    public String getAccountBuyerSeller() {
        return accountBuyerSeller;
    }

    public void setAccountBuyerSeller(String accountBuyerSeller) {
        this.accountBuyerSeller = accountBuyerSeller;
    }

    public GenMdAccountEntity(Long accountId, String accountCode, Long memberId, String clientCode,
                              String accountType, String checkDigit, String accountName, String accountPosition,
                              String correspondAcNo, String blockingReason, String accountStatus, String createdBy,
                              Date createdOn, String modifiedBy, Date modifiedOn, String checkedBy,
                              Date checkedOn, String approvedBy, Date approvedOn, String accountBuyerSeller) {
        this.accountId = accountId;
        this.accountCode = accountCode;
        this.memberId = memberId;
        this.clientCode = clientCode;
        this.accountType = accountType;
        this.checkDigit = checkDigit;
        this.accountName = accountName;
        this.accountPosition = accountPosition;
        this.correspondAcNo = correspondAcNo;
        this.blockingReason = blockingReason;
        this.accountStatus = accountStatus;
        this.createdBy = createdBy;
        this.createdOn = createdOn;
        this.modifiedBy = modifiedBy;
        this.modifiedOn = modifiedOn;
        this.checkedBy = checkedBy;
        this.checkedOn = checkedOn;
        this.approvedBy = approvedBy;
        this.approvedOn = approvedOn;
        this.accountBuyerSeller = accountBuyerSeller;
    }

    public GenMdAccountEntity() {
    }
}