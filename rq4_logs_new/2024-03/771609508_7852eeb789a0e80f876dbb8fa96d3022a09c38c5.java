package com.oasis.poc1.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
public class Attributes {
	
	private Integer OBJECTID;
	private Integer OGF_ID;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Integer WELL_ID;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String LICENCE_NUMBER;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String WELL_NAME;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String OPERATOR;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String WELL_TYPE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String WELL_MODE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double SURFACE_LATITUDE_83;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double SURFACE_LONGITUDE_83;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double BOTTOM_LATITUDE_83;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double BOTTOM_LONGITUDE_83;
	private String LOCATION_ACCURACY;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String LOCATION_SOURCE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double GROUND_ELEVATION;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double TOTAL_DEPTH;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Double TRUE_VERTICAL_DEPTH;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String COUNTY;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String TOWNSHIP;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String LOT;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String CONCESSION;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String NORTH_SOUTH_BOUNDARY;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String NORTH_SOUTH_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String EAST_WEST_BOUNDARY;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String EAST_WEST_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String OFFSHORE_BLOCK;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String OFFSHORE_TRACT;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String VERTICAL_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String HORIZONTAL_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String DIRECTIONAL_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String DEEPENING_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String REENTRY_FLG;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String LATERAL_FLG;	
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Long SPUD_DATE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Long TOTAL_DEPTH_REACHED_DATE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Long PLUGGING_END_DATE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String MAP_SYMBOL_CODE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String TILE_SYSTEM_TYPE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String JOB_NUMBER;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String NEW_TILE_SYSTEM_IND;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Integer YEAR_OF_INSTALLATION;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String TILE_CONTRACTOR_NAME;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String TILE_CONTRACTOR_LICENCE_NUMBER;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Integer AREA_TILED_ACRES;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String AREA_TILED_HECTARES;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String AREA_TILED_SOURCE;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String TYPE_OTHER_DESCR;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Integer TILE_LENGTH_FEET;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private Integer TILE_LENGTH_METRES;
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String GENERAL_COMMENTS;	
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	private String REPORT_URL;
	private Long GEOMETRY_UPDATE_DATETIME;
	private Long EFFECTIVE_DATETIME;
	private Long SYSTEM_DATETIME;
	private String SE_ANNO_CAD_DATA;
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Double Shape__Area;
	@JsonInclude(JsonInclude.Include.NON_ABSENT)
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
	public Integer getWELL_ID() {
		return WELL_ID;
	}
	public void setWELL_ID(Integer wELL_ID) {
		WELL_ID = wELL_ID;
	}
	public String getLICENCE_NUMBER() {
		return LICENCE_NUMBER;
	}
	public void setLICENCE_NUMBER(String lICENCE_NUMBER) {
		LICENCE_NUMBER = lICENCE_NUMBER;
	}
	public String getWELL_NAME() {
		return WELL_NAME;
	}
	public void setWELL_NAME(String wELL_NAME) {
		WELL_NAME = wELL_NAME;
	}
	public String getOPERATOR() {
		return OPERATOR;
	}
	public void setOPERATOR(String oPERATOR) {
		OPERATOR = oPERATOR;
	}
	public String getWELL_TYPE() {
		return WELL_TYPE;
	}
	public void setWELL_TYPE(String wELL_TYPE) {
		WELL_TYPE = wELL_TYPE;
	}
	public String getWELL_MODE() {
		return WELL_MODE;
	}
	public void setWELL_MODE(String wELL_MODE) {
		WELL_MODE = wELL_MODE;
	}
	public Double getSURFACE_LATITUDE_83() {
		return SURFACE_LATITUDE_83;
	}
	public void setSURFACE_LATITUDE_83(Double sURFACE_LATITUDE_83) {
		SURFACE_LATITUDE_83 = sURFACE_LATITUDE_83;
	}
	public Double getSURFACE_LONGITUDE_83() {
		return SURFACE_LONGITUDE_83;
	}
	public void setSURFACE_LONGITUDE_83(Double sURFACE_LONGITUDE_83) {
		SURFACE_LONGITUDE_83 = sURFACE_LONGITUDE_83;
	}
	public Double getBOTTOM_LATITUDE_83() {
		return BOTTOM_LATITUDE_83;
	}
	public void setBOTTOM_LATITUDE_83(Double bOTTOM_LATITUDE_83) {
		BOTTOM_LATITUDE_83 = bOTTOM_LATITUDE_83;
	}
	public Double getBOTTOM_LONGITUDE_83() {
		return BOTTOM_LONGITUDE_83;
	}
	public void setBOTTOM_LONGITUDE_83(Double bOTTOM_LONGITUDE_83) {
		BOTTOM_LONGITUDE_83 = bOTTOM_LONGITUDE_83;
	}
	public String getLOCATION_ACCURACY() {
		return LOCATION_ACCURACY;
	}
	public void setLOCATION_ACCURACY(String lOCATION_ACCURACY) {
		LOCATION_ACCURACY = lOCATION_ACCURACY;
	}
	public String getLOCATION_SOURCE() {
		return LOCATION_SOURCE;
	}
	public void setLOCATION_SOURCE(String lOCATION_SOURCE) {
		LOCATION_SOURCE = lOCATION_SOURCE;
	}
	public Double getGROUND_ELEVATION() {
		return GROUND_ELEVATION;
	}
	public void setGROUND_ELEVATION(Double gROUND_ELEVATION) {
		GROUND_ELEVATION = gROUND_ELEVATION;
	}
	public Double getTOTAL_DEPTH() {
		return TOTAL_DEPTH;
	}
	public void setTOTAL_DEPTH(Double tOTAL_DEPTH) {
		TOTAL_DEPTH = tOTAL_DEPTH;
	}
	public Double getTRUE_VERTICAL_DEPTH() {
		return TRUE_VERTICAL_DEPTH;
	}
	public void setTRUE_VERTICAL_DEPTH(Double tRUE_VERTICAL_DEPTH) {
		TRUE_VERTICAL_DEPTH = tRUE_VERTICAL_DEPTH;
	}
	public String getCOUNTY() {
		return COUNTY;
	}
	public void setCOUNTY(String cOUNTY) {
		COUNTY = cOUNTY;
	}
	public String getTOWNSHIP() {
		return TOWNSHIP;
	}
	public void setTOWNSHIP(String tOWNSHIP) {
		TOWNSHIP = tOWNSHIP;
	}
	public String getLOT() {
		return LOT;
	}
	public void setLOT(String lOT) {
		LOT = lOT;
	}
	public String getCONCESSION() {
		return CONCESSION;
	}
	public void setCONCESSION(String cONCESSION) {
		CONCESSION = cONCESSION;
	}
	public String getNORTH_SOUTH_BOUNDARY() {
		return NORTH_SOUTH_BOUNDARY;
	}
	public void setNORTH_SOUTH_BOUNDARY(String nORTH_SOUTH_BOUNDARY) {
		NORTH_SOUTH_BOUNDARY = nORTH_SOUTH_BOUNDARY;
	}
	public String getNORTH_SOUTH_FLG() {
		return NORTH_SOUTH_FLG;
	}
	public void setNORTH_SOUTH_FLG(String nORTH_SOUTH_FLG) {
		NORTH_SOUTH_FLG = nORTH_SOUTH_FLG;
	}
	public String getEAST_WEST_BOUNDARY() {
		return EAST_WEST_BOUNDARY;
	}
	public void setEAST_WEST_BOUNDARY(String eAST_WEST_BOUNDARY) {
		EAST_WEST_BOUNDARY = eAST_WEST_BOUNDARY;
	}
	public String getEAST_WEST_FLG() {
		return EAST_WEST_FLG;
	}
	public void setEAST_WEST_FLG(String eAST_WEST_FLG) {
		EAST_WEST_FLG = eAST_WEST_FLG;
	}
	public String getOFFSHORE_BLOCK() {
		return OFFSHORE_BLOCK;
	}
	public void setOFFSHORE_BLOCK(String oFFSHORE_BLOCK) {
		OFFSHORE_BLOCK = oFFSHORE_BLOCK;
	}
	public String getOFFSHORE_TRACT() {
		return OFFSHORE_TRACT;
	}
	public void setOFFSHORE_TRACT(String oFFSHORE_TRACT) {
		OFFSHORE_TRACT = oFFSHORE_TRACT;
	}
	public String getVERTICAL_FLG() {
		return VERTICAL_FLG;
	}
	public void setVERTICAL_FLG(String vERTICAL_FLG) {
		VERTICAL_FLG = vERTICAL_FLG;
	}
	public String getHORIZONTAL_FLG() {
		return HORIZONTAL_FLG;
	}
	public void setHORIZONTAL_FLG(String hORIZONTAL_FLG) {
		HORIZONTAL_FLG = hORIZONTAL_FLG;
	}
	public String getDIRECTIONAL_FLG() {
		return DIRECTIONAL_FLG;
	}
	public void setDIRECTIONAL_FLG(String dIRECTIONAL_FLG) {
		DIRECTIONAL_FLG = dIRECTIONAL_FLG;
	}
	public String getDEEPENING_FLG() {
		return DEEPENING_FLG;
	}
	public void setDEEPENING_FLG(String dEEPENING_FLG) {
		DEEPENING_FLG = dEEPENING_FLG;
	}
	public String getREENTRY_FLG() {
		return REENTRY_FLG;
	}
	public void setREENTRY_FLG(String rEENTRY_FLG) {
		REENTRY_FLG = rEENTRY_FLG;
	}
	public String getLATERAL_FLG() {
		return LATERAL_FLG;
	}
	public void setLATERAL_FLG(String lATERAL_FLG) {
		LATERAL_FLG = lATERAL_FLG;
	}
	public Long getSPUD_DATE() {
		return SPUD_DATE;
	}
	public void setSPUD_DATE(Long sPUD_DATE) {
		SPUD_DATE = sPUD_DATE;
	}
	public Long getTOTAL_DEPTH_REACHED_DATE() {
		return TOTAL_DEPTH_REACHED_DATE;
	}
	public void setTOTAL_DEPTH_REACHED_DATE(Long tOTAL_DEPTH_REACHED_DATE) {
		TOTAL_DEPTH_REACHED_DATE = tOTAL_DEPTH_REACHED_DATE;
	}
	public Long getPLUGGING_END_DATE() {
		return PLUGGING_END_DATE;
	}
	public void setPLUGGING_END_DATE(Long pLUGGING_END_DATE) {
		PLUGGING_END_DATE = pLUGGING_END_DATE;
	}
	public String getMAP_SYMBOL_CODE() {
		return MAP_SYMBOL_CODE;
	}
	public void setMAP_SYMBOL_CODE(String mAP_SYMBOL_CODE) {
		MAP_SYMBOL_CODE = mAP_SYMBOL_CODE;
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
		return "Attributes [OBJECTID=" + OBJECTID + ", OGF_ID=" + OGF_ID + ", WELL_ID=" + WELL_ID + ", LICENCE_NUMBER="
				+ LICENCE_NUMBER + ", WELL_NAME=" + WELL_NAME + ", OPERATOR=" + OPERATOR + ", WELL_TYPE=" + WELL_TYPE
				+ ", WELL_MODE=" + WELL_MODE + ", SURFACE_LATITUDE_83=" + SURFACE_LATITUDE_83
				+ ", SURFACE_LONGITUDE_83=" + SURFACE_LONGITUDE_83 + ", BOTTOM_LATITUDE_83=" + BOTTOM_LATITUDE_83
				+ ", BOTTOM_LONGITUDE_83=" + BOTTOM_LONGITUDE_83 + ", LOCATION_ACCURACY=" + LOCATION_ACCURACY
				+ ", LOCATION_SOURCE=" + LOCATION_SOURCE + ", GROUND_ELEVATION=" + GROUND_ELEVATION + ", TOTAL_DEPTH="
				+ TOTAL_DEPTH + ", TRUE_VERTICAL_DEPTH=" + TRUE_VERTICAL_DEPTH + ", COUNTY=" + COUNTY + ", TOWNSHIP="
				+ TOWNSHIP + ", LOT=" + LOT + ", CONCESSION=" + CONCESSION + ", NORTH_SOUTH_BOUNDARY="
				+ NORTH_SOUTH_BOUNDARY + ", NORTH_SOUTH_FLG=" + NORTH_SOUTH_FLG + ", EAST_WEST_BOUNDARY="
				+ EAST_WEST_BOUNDARY + ", EAST_WEST_FLG=" + EAST_WEST_FLG + ", OFFSHORE_BLOCK=" + OFFSHORE_BLOCK
				+ ", OFFSHORE_TRACT=" + OFFSHORE_TRACT + ", VERTICAL_FLG=" + VERTICAL_FLG + ", HORIZONTAL_FLG="
				+ HORIZONTAL_FLG + ", DIRECTIONAL_FLG=" + DIRECTIONAL_FLG + ", DEEPENING_FLG=" + DEEPENING_FLG
				+ ", REENTRY_FLG=" + REENTRY_FLG + ", LATERAL_FLG=" + LATERAL_FLG + ", SPUD_DATE=" + SPUD_DATE
				+ ", TOTAL_DEPTH_REACHED_DATE=" + TOTAL_DEPTH_REACHED_DATE + ", PLUGGING_END_DATE=" + PLUGGING_END_DATE
				+ ", MAP_SYMBOL_CODE=" + MAP_SYMBOL_CODE + ", TILE_SYSTEM_TYPE=" + TILE_SYSTEM_TYPE + ", JOB_NUMBER="
				+ JOB_NUMBER + ", NEW_TILE_SYSTEM_IND=" + NEW_TILE_SYSTEM_IND + ", YEAR_OF_INSTALLATION="
				+ YEAR_OF_INSTALLATION + ", TILE_CONTRACTOR_NAME=" + TILE_CONTRACTOR_NAME
				+ ", TILE_CONTRACTOR_LICENCE_NUMBER=" + TILE_CONTRACTOR_LICENCE_NUMBER + ", AREA_TILED_ACRES="
				+ AREA_TILED_ACRES + ", AREA_TILED_HECTARES=" + AREA_TILED_HECTARES + ", AREA_TILED_SOURCE="
				+ AREA_TILED_SOURCE + ", TYPE_OTHER_DESCR=" + TYPE_OTHER_DESCR + ", TILE_LENGTH_FEET="
				+ TILE_LENGTH_FEET + ", TILE_LENGTH_METRES=" + TILE_LENGTH_METRES + ", GENERAL_COMMENTS="
				+ GENERAL_COMMENTS + ", REPORT_URL=" + REPORT_URL + ", GEOMETRY_UPDATE_DATETIME="
				+ GEOMETRY_UPDATE_DATETIME + ", EFFECTIVE_DATETIME=" + EFFECTIVE_DATETIME + ", SYSTEM_DATETIME="
				+ SYSTEM_DATETIME + ", SE_ANNO_CAD_DATA=" + SE_ANNO_CAD_DATA + ", Shape__Area=" + Shape__Area + ", Shape__Length=" + Shape__Length + "]";
	}
	
	
	
}