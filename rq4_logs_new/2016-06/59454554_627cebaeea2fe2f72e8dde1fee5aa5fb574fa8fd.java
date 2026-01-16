package org.house.db.entity.solr;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.solr.client.solrj.beans.Field;
import org.house.util.C;
import org.springframework.data.solr.core.mapping.SolrDocument;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;

@JsonAutoDetect
@SolrDocument(solrCoreName = "collection1")
public class EarthBasicDataSolr extends AbstractSolrBean implements Serializable {
	private static final long serialVersionUID = 1L;
	@Field("earth_license_id")
	protected String earthLicenseId; // 国土证号ID
	@Field("project_id")
	protected String projectId;
	@Field("earth_license_no")
	protected String earthLicenseNo; // 国土证号
	@Field("location")
	protected String location; // 土地座落
	@Field("userr")
	protected String userr; // 土地使用者
	@Field("earth_no")
	protected String earthNo; // 地号
	@Field("graph_no")
	protected String graphNo;// 图号
	@Field("usagee")
	protected String usagee;// 土地用途
	@Field("levell")
	protected String levell; // 土地等级
	@Temporal(TemporalType.DATE)
	@JsonSerialize(using = DateSerializer.class)
	@JsonFormat(pattern = C.DATE_JSON_FORMAT_PATTERN, timezone = C.DATE_JSON_FORMAT_TIMEZONE)
	@Field("borrow_from")
	protected Date borrowFrom; // 土地出让年限自
	@Field("use_right_kind")
	protected String useRightKind; // 使用权类型
	@Field("use_area")
	protected String useArea; // 使用权面积
	@Field("share_area")
	protected String shareArea; // 其中共用分推面积
	@Field("license_office")
	protected String licenseOffice; // 发证机关
	@Temporal(TemporalType.DATE)
	@JsonSerialize(using = DateSerializer.class)
	@JsonFormat(pattern = C.DATE_JSON_FORMAT_PATTERN, timezone = C.DATE_JSON_FORMAT_TIMEZONE)
	@Field("license_issue_date")
	protected Date licenseIssueDate; // 发证日期

	@Override
	public int hashCode() {
		int result = 17;
		if (this.earthLicenseId != null) {
			result = 31 * result + this.earthLicenseId.hashCode();
		}
		if (this.projectId != null) {
			result = 31 * result + this.projectId.hashCode();
		}
		if (this.earthLicenseNo != null) {
			result = 31 * result + this.earthLicenseNo.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof EarthBasicDataSolr) {
			final EarthBasicDataSolr anObject = (EarthBasicDataSolr) obj;
			if (ObjectUtils.nullSafeEquals(this.earthLicenseId, anObject.getEarthLicenseId())
					&& ObjectUtils.nullSafeEquals(this.projectId, anObject.getProjectId())
					&& ObjectUtils.nullSafeEquals(this.earthLicenseNo, anObject.getEarthLicenseNo())
					&& ObjectUtils.nullSafeEquals(this.location, anObject.getLocation())
					&& ObjectUtils.nullSafeEquals(this.userr, anObject.getUserr()) && ObjectUtils.nullSafeEquals(this.earthNo, anObject.getEarthNo())
					&& ObjectUtils.nullSafeEquals(this.graphNo, anObject.getGraphNo())
					&& ObjectUtils.nullSafeEquals(this.usagee, anObject.getUsagee()) && ObjectUtils.nullSafeEquals(this.levell, anObject.getLevell())
					&& ObjectUtils.nullSafeEquals(this.borrowFrom, anObject.getBorrowFrom())
					&& ObjectUtils.nullSafeEquals(this.useRightKind, anObject.getUseRightKind())
					&& ObjectUtils.nullSafeEquals(this.useArea, anObject.getUseArea())
					&& ObjectUtils.nullSafeEquals(this.shareArea, anObject.getShareArea())
					&& ObjectUtils.nullSafeEquals(this.licenseOffice, anObject.getLicenseOffice())
					&& ObjectUtils.nullSafeEquals(this.licenseIssueDate, anObject.getLicenseIssueDate())) {
				return true;
			}
		}
		return false;
	}

	public void fromObj(final EarthBasicDataSolr theObj) {
		// this.earthLicenseId = theObj.earthLicenseId; // NO ID
		this.projectId = theObj.projectId;
		this.earthLicenseNo = theObj.earthLicenseNo;
		this.location = theObj.location;
		this.userr = theObj.userr;
		this.earthNo = theObj.earthNo;
		this.graphNo = theObj.graphNo;
		this.usagee = theObj.usagee;
		this.levell = theObj.levell;
		this.borrowFrom = theObj.borrowFrom;
		this.useRightKind = theObj.useRightKind;
		this.useArea = theObj.useArea;
		this.shareArea = theObj.shareArea;
		this.licenseOffice = theObj.licenseOffice;
		this.licenseIssueDate = theObj.licenseIssueDate;
	}

	public String getEarthLicenseId() {
		return this.earthLicenseId;
	}

	public void setEarthLicenseId(final String earthLicenseId) {
		this.earthLicenseId = earthLicenseId;
	}

	public String getProjectId() {
		return this.projectId;
	}

	public void setProjectId(final String projectId) {
		this.projectId = projectId;
	}

	public String getEarthLicenseNo() {
		return this.earthLicenseNo;
	}

	public void setEarthLicenseNo(final String earthLicenseNo) {
		this.earthLicenseNo = earthLicenseNo;
	}

	public String getLocation() {
		return this.location;
	}

	public void setLocation(final String location) {
		this.location = location;
	}

	public String getUserr() {
		return this.userr;
	}

	public void setUserr(final String userr) {
		this.userr = userr;
	}

	public String getEarthNo() {
		return this.earthNo;
	}

	public void setEarthNo(final String earthNo) {
		this.earthNo = earthNo;
	}

	public String getGraphNo() {
		return this.graphNo;
	}

	public void setGraphNo(final String graphNo) {
		this.graphNo = graphNo;
	}

	public String getUsagee() {
		return this.usagee;
	}

	public void setUsagee(final String usagee) {
		this.usagee = usagee;
	}

	public String getLevell() {
		return this.levell;
	}

	public void setLevell(final String levell) {
		this.levell = levell;
	}

	public Date getBorrowFrom() {
		return this.borrowFrom;
	}

	public void setBorrowFrom(final Date borrowFrom) {
		this.borrowFrom = borrowFrom;
	}

	public String getUseRightKind() {
		return this.useRightKind;
	}

	public void setUseRightKind(final String useRightKind) {
		this.useRightKind = useRightKind;
	}

	public String getUseArea() {
		return this.useArea;
	}

	public void setUseArea(final String useArea) {
		this.useArea = useArea;
	}

	public String getShareArea() {
		return this.shareArea;
	}

	public void setShareArea(final String shareArea) {
		this.shareArea = shareArea;
	}

	public String getLicenseOffice() {
		return this.licenseOffice;
	}

	public void setLicenseOffice(final String licenseOffice) {
		this.licenseOffice = licenseOffice;
	}

	public Date getLicenseIssueDate() {
		return this.licenseIssueDate;
	}

	public void setLicenseIssueDate(final Date licenseIssueDate) {
		this.licenseIssueDate = licenseIssueDate;
	}
}