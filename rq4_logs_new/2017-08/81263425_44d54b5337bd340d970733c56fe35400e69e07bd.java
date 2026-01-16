package com.bringit.experiment.bll;

import java.util.Date;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

@Entity
@Table(name="SystemSettings")
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region="entity")
@Cacheable
public class SystemSettings {
	
	@Id
	@GeneratedValue(strategy= GenerationType.AUTO)
	@Column(name="SystemSettingsId")
	private Integer systemSettingsId;

	@Column(name="ExperimentLabel")
	String experimentLabel;
	
	@Column(name="ExperimentPluralLabel")
	String experimentPluralLabel;
	
	@Column(name="ExperimentTypeLabel")
	String experimentTypeLabel;

	@Column(name="ExperimentTypePluralLabel")
	String experimentTypePluralLabel;
	
    @Column(name="CreatedDate")
    private Date createdDate;

    @Column(name="ModifiedDate")
    private Date modifiedDate;
    
    @OneToOne(fetch= FetchType.LAZY)
    @JoinColumn(name="CreatedBy", unique=false, updatable=false)
    private SysUser createdBy;

    @OneToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="LastModifiedBy", unique=false, updatable=true)
    private SysUser lastModifiedBy;

    public SystemSettings() {}

	public SystemSettings(Integer systemSettingsId, String experimentLabel, String experimentPluralLabel,
			String experimentTypeLabel, String experimentTypePluralLabel, Date createdDate, Date modifiedDate, SysUser createdBy,
			SysUser lastModifiedBy) {
		this.systemSettingsId = systemSettingsId;
		this.experimentLabel = experimentLabel;
		this.experimentPluralLabel = experimentPluralLabel;
		this.experimentTypeLabel = experimentTypeLabel;
		this.experimentTypePluralLabel = experimentTypePluralLabel;
		this.createdDate = createdDate;
		this.modifiedDate = modifiedDate;
		this.createdBy = createdBy;
		this.lastModifiedBy = lastModifiedBy;
	}

	public Integer getSystemSettingsId() {
		return systemSettingsId;
	}

	public void setSystemSettingsId(Integer systemSettingsId) {
		this.systemSettingsId = systemSettingsId;
	}

	public String getExperimentLabel() {
		return experimentLabel;
	}

	public void setExperimentLabel(String experimentLabel) {
		this.experimentLabel = experimentLabel;
	}

	public String getExperimentPluralLabel() {
		return experimentPluralLabel;
	}

	public void setExperimentPluralLabel(String experimentPluralLabel) {
		this.experimentPluralLabel = experimentPluralLabel;
	}

	public String getExperimentTypeLabel() {
		return experimentTypeLabel;
	}

	public void setExperimentTypeLabel(String experimentTypeLabel) {
		this.experimentTypeLabel = experimentTypeLabel;
	}

	public String getExperimentTypePluralLabel() {
		return experimentTypePluralLabel;
	}

	public void setExperimentTypePluralLabel(String experimentTypePluralLabel) {
		this.experimentTypePluralLabel = experimentTypePluralLabel;
	}

	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createdDate) {
		this.createdDate = createdDate;
	}

	public Date getModifiedDate() {
		return modifiedDate;
	}

	public void setModifiedDate(Date modifiedDate) {
		this.modifiedDate = modifiedDate;
	}

	public SysUser getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(SysUser createdBy) {
		this.createdBy = createdBy;
	}

	public SysUser getLastModifiedBy() {
		return lastModifiedBy;
	}

	public void setLastModifiedBy(SysUser lastModifiedBy) {
		this.lastModifiedBy = lastModifiedBy;
	}   
    
}