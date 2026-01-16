package com.oasis.poc1.entity;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
public class TileAttributes {

	private Integer OBJECTID;
	private Integer OGF_ID;
	
	//Tile Attributes
	
	private String TILE_SYSTEM_TYPE;	
	private String JOB_NUMBER;	
	private String NEW_TILE_SYSTEM_IND;	
	private Integer YEAR_OF_INSTALLATION;	
	private String TILE_CONTRACTOR_NAME;	
	private String TILE_CONTRACTOR_LICENCE_NUMBER;	
	private Integer AREA_TILED_ACRES;	
	private String AREA_TILED_HECTARES;	
	private String AREA_TILED_SOURCE;	
	private String TYPE_OTHER_DESCR;	
	private Integer TILE_LENGTH_FEET;	
	private Integer TILE_LENGTH_METRES;	
	private String GENERAL_COMMENTS;		
	private String REPORT_URL;
	
	private String LOCATION_ACCURACY;
	private Long GEOMETRY_UPDATE_DATETIME;
	private Long EFFECTIVE_DATETIME;
	private Long SYSTEM_DATETIME;
	private String SE_ANNO_CAD_DATA;
	
	private Double Shape__Area;
	private Double Shape__Length;
	public Integer getOBJECTID() {
		return OBJECTID;
	}
	public void setOBJECTID(Integer oBJECTID) {
		OBJECTID = oBJECTID;
	}
	public Integer getOGF_ID() {
		return OGF_ID;
	}
	public void setOGF_ID(Integer oGF_ID) {
		OGF_ID = oGF_ID;
	}
	public String getTILE_SYSTEM_TYPE() {
		return TILE_SYSTEM_TYPE;
	}
	public void setTILE_SYSTEM_TYPE(String tILE_SYSTEM_TYPE) {
		TILE_SYSTEM_TYPE = tILE_SYSTEM_TYPE;
	}
	public String getJOB_NUMBER() {
		return JOB_NUMBER;
	}
	public void setJOB_NUMBER(String jOB_NUMBER) {
		JOB_NUMBER = jOB_NUMBER;
	}
	public String getNEW_TILE_SYSTEM_IND() {
		return NEW_TILE_SYSTEM_IND;
	}
	public void setNEW_TILE_SYSTEM_IND(String nEW_TILE_SYSTEM_IND) {
		NEW_TILE_SYSTEM_IND = nEW_TILE_SYSTEM_IND;
	}
	public Integer getYEAR_OF_INSTALLATION() {
		return YEAR_OF_INSTALLATION;
	}
	public void setYEAR_OF_INSTALLATION(Integer yEAR_OF_INSTALLATION) {
		YEAR_OF_INSTALLATION = yEAR_OF_INSTALLATION;
	}
	public String getTILE_CONTRACTOR_NAME() {
		return TILE_CONTRACTOR_NAME;
	}
	public void setTILE_CONTRACTOR_NAME(String tILE_CONTRACTOR_NAME) {
		TILE_CONTRACTOR_NAME = tILE_CONTRACTOR_NAME;
	}
	public String getTILE_CONTRACTOR_LICENCE_NUMBER() {
		return TILE_CONTRACTOR_LICENCE_NUMBER;
	}
	public void setTILE_CONTRACTOR_LICENCE_NUMBER(String tILE_CONTRACTOR_LICENCE_NUMBER) {
		TILE_CONTRACTOR_LICENCE_NUMBER = tILE_CONTRACTOR_LICENCE_NUMBER;
	}
	public Integer getAREA_TILED_ACRES() {
		return AREA_TILED_ACRES;
	}
	public void setAREA_TILED_ACRES(Integer aREA_TILED_ACRES) {
		AREA_TILED_ACRES = aREA_TILED_ACRES;
	}
	public String getAREA_TILED_HECTARES() {
		return AREA_TILED_HECTARES;
	}
	public void setAREA_TILED_HECTARES(String aREA_TILED_HECTARES) {
		AREA_TILED_HECTARES = aREA_TILED_HECTARES;
	}
	public String getAREA_TILED_SOURCE() {
		return AREA_TILED_SOURCE;
	}
	public void setAREA_TILED_SOURCE(String aREA_TILED_SOURCE) {
		AREA_TILED_SOURCE = aREA_TILED_SOURCE;
	}
	public String getTYPE_OTHER_DESCR() {
		return TYPE_OTHER_DESCR;
	}
	public void setTYPE_OTHER_DESCR(String tYPE_OTHER_DESCR) {
		TYPE_OTHER_DESCR = tYPE_OTHER_DESCR;
	}
	public Integer getTILE_LENGTH_FEET() {
		return TILE_LENGTH_FEET;
	}
	public void setTILE_LENGTH_FEET(Integer tILE_LENGTH_FEET) {
		TILE_LENGTH_FEET = tILE_LENGTH_FEET;
	}
	public Integer getTILE_LENGTH_METRES() {
		return TILE_LENGTH_METRES;
	}
	public void setTILE_LENGTH_METRES(Integer tILE_LENGTH_METRES) {
		TILE_LENGTH_METRES = tILE_LENGTH_METRES;
	}
	public String getGENERAL_COMMENTS() {
		return GENERAL_COMMENTS;
	}
	public void setGENERAL_COMMENTS(String gENERAL_COMMENTS) {
		GENERAL_COMMENTS = gENERAL_COMMENTS;
	}
	public String getREPORT_URL() {
		return REPORT_URL;
	}
	public void setREPORT_URL(String rEPORT_URL) {
		REPORT_URL = rEPORT_URL;
	}
	public String getLOCATION_ACCURACY() {
		return LOCATION_ACCURACY;
	}
	public void setLOCATION_ACCURACY(String lOCATION_ACCURACY) {
		LOCATION_ACCURACY = lOCATION_ACCURACY;
	}
	public Long getGEOMETRY_UPDATE_DATETIME() {
		return GEOMETRY_UPDATE_DATETIME;
	}
	public void setGEOMETRY_UPDATE_DATETIME(Long gEOMETRY_UPDATE_DATETIME) {
		GEOMETRY_UPDATE_DATETIME = gEOMETRY_UPDATE_DATETIME;
	}
	public Long getEFFECTIVE_DATETIME() {
		return EFFECTIVE_DATETIME;
	}
	public void setEFFECTIVE_DATETIME(Long eFFECTIVE_DATETIME) {
		EFFECTIVE_DATETIME = eFFECTIVE_DATETIME;
	}
	public Long getSYSTEM_DATETIME() {
		return SYSTEM_DATETIME;
	}
	public void setSYSTEM_DATETIME(Long sYSTEM_DATETIME) {
		SYSTEM_DATETIME = sYSTEM_DATETIME;
	}
	public String getSE_ANNO_CAD_DATA() {
		return SE_ANNO_CAD_DATA;
	}
	public void setSE_ANNO_CAD_DATA(String sE_ANNO_CAD_DATA) {
		SE_ANNO_CAD_DATA = sE_ANNO_CAD_DATA;
	}
	public Double getShape__Area() {
		return Shape__Area;
	}
	public void setShape__Area(Double shape__Area) {
		Shape__Area = shape__Area;
	}
	public Double getShape__Length() {
		return Shape__Length;
	}
	public void setShape__Length(Double shape__Length) {
		Shape__Length = shape__Length;
	}
	
	@Override
	public String toString() {
		return "TileAttributes [OBJECTID=" + OBJECTID + ", OGF_ID=" + OGF_ID + ", TILE_SYSTEM_TYPE=" + TILE_SYSTEM_TYPE
				+ ", JOB_NUMBER=" + JOB_NUMBER + ", NEW_TILE_SYSTEM_IND=" + NEW_TILE_SYSTEM_IND
				+ ", YEAR_OF_INSTALLATION=" + YEAR_OF_INSTALLATION + ", TILE_CONTRACTOR_NAME=" + TILE_CONTRACTOR_NAME
				+ ", TILE_CONTRACTOR_LICENCE_NUMBER=" + TILE_CONTRACTOR_LICENCE_NUMBER + ", AREA_TILED_ACRES="
				+ AREA_TILED_ACRES + ", AREA_TILED_HECTARES=" + AREA_TILED_HECTARES + ", AREA_TILED_SOURCE="
				+ AREA_TILED_SOURCE + ", TYPE_OTHER_DESCR=" + TYPE_OTHER_DESCR + ", TILE_LENGTH_FEET="
				+ TILE_LENGTH_FEET + ", TILE_LENGTH_METRES=" + TILE_LENGTH_METRES + ", GENERAL_COMMENTS="
				+ GENERAL_COMMENTS + ", REPORT_URL=" + REPORT_URL + ", LOCATION_ACCURACY=" + LOCATION_ACCURACY
				+ ", GEOMETRY_UPDATE_DATETIME=" + GEOMETRY_UPDATE_DATETIME + ", EFFECTIVE_DATETIME="
				+ EFFECTIVE_DATETIME + ", SYSTEM_DATETIME=" + SYSTEM_DATETIME + ", SE_ANNO_CAD_DATA=" + SE_ANNO_CAD_DATA
				+ ", Shape__Area=" + Shape__Area + ", Shape__Length=" + Shape__Length + "]";
	}
	
	



}