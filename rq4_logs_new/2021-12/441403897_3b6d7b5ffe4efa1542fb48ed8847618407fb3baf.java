package com.spartenportal.bean;

import java.util.Date;

public class AttendanceSheetBean {
	private Integer sheetId;

	private String sheetName;
	
	private String sheetType;

	private Date uploadDate;
	
	private Date modifyDate;
	
	private byte[] sheet;
	
	private Integer useridFK;

	public Integer getSheetId() {
		return sheetId;
	}

	public void setSheetId(Integer sheetId) {
		this.sheetId = sheetId;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getSheetType() {
		return sheetType;
	}

	public void setSheetType(String sheetType) {
		this.sheetType = sheetType;
	}

	public Date getUploadDate() {
		return uploadDate;
	}

	public void setUploadDate(Date uploadDate) {
		this.uploadDate = uploadDate;
	}

	public Date getModifyDate() {
		return modifyDate;
	}

	public void setModifyDate(Date modifyDate) {
		this.modifyDate = modifyDate;
	}

	public byte[] getSheet() {
		return sheet;
	}

	public void setSheet(byte[] sheet) {
		this.sheet = sheet;
	}

	public Integer getUseridFK() {
		return useridFK;
	}

	public void setUseridFK(Integer useridFK) {
		this.useridFK = useridFK;
	}
}