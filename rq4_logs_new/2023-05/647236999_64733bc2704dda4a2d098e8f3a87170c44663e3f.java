import java.util.Date;

public class AltGroupParameterValueAggregate{
    private Long altGroupParameterValueID;
    private Integer altGroupID;
    private Integer organizationID;
    private Integer tenantID;
    private Long sysGroupParameterID;
    private String valueList;
    private Boolean isInclusive;
    private Boolean isActive;
    private Integer createdBy;
    private Date createdDate;
    private Integer modifiedBy;
    private Date modifiedDate;

    public AltGroupParameterValueAggregate() {
    }

    public Long getAltGroupParameterValueID() {
        return altGroupParameterValueID;
    }

    public void setAltGroupParameterValueID(Long altGroupParameterValueID) {
        this.altGroupParameterValueID = altGroupParameterValueID;
    }

    public Integer getAltGroupID() {
        return altGroupID;
    }

    public void setAltGroupID(Integer altGroupID) {
        this.altGroupID = altGroupID;
    }

    public Integer getOrganizationID() {
        return organizationID;
    }

    public void setOrganizationID(Integer organizationID) {
        this.organizationID = organizationID;
    }

    public Integer getTenantID() {
        return tenantID;
    }

    public void setTenantID(Integer tenantID) {
        this.tenantID = tenantID;
    }

    public Long getSysGroupParameterID() {
        return sysGroupParameterID;
    }

    public void setSysGroupParameterID(Long sysGroupParameterID) {
        this.sysGroupParameterID = sysGroupParameterID;
    }

    public String getValueList() {
        return valueList;
    }

    public void setValueList(String valueList) {
        this.valueList = valueList;
    }

    public Boolean getIsInclusive() {
        return isInclusive;
    }

    public void setIsInclusive(Boolean isInclusive) {
        this.isInclusive = isInclusive;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public Integer getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(Integer createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public Integer getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(Integer modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public Date getModifiedDate() {
        return modifiedDate;
    }

    public void setModifiedDate(Date modifiedDate) {
        this.modifiedDate = modifiedDate;
    }
}