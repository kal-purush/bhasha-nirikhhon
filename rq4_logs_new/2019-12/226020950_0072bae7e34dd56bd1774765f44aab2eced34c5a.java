package com.min.edu.dto;

public class AllObj_Dto {
	
	String main_obj       ;
	String obj_code       ;
	String obj_content    ;
	String obj_detail_code;
	public String getMain_obj() {
		return main_obj;
	}
	public void setMain_obj(String main_obj) {
		this.main_obj = main_obj;
	}
	public String getObj_code() {
		return obj_code;
	}
	public void setObj_code(String obj_code) {
		this.obj_code = obj_code;
	}
	public String getObj_content() {
		return obj_content;
	}
	public void setObj_content(String obj_content) {
		this.obj_content = obj_content;
	}
	public String getObj_detail_code() {
		return obj_detail_code;
	}
	public void setObj_detail_code(String obj_detail_code) {
		this.obj_detail_code = obj_detail_code;
	}
	
	public AllObj_Dto() {
		// TODO Auto-generated constructor stub
	}
	@Override
	public String toString() {
		return "AllObj_Dto [main_obj=" + main_obj + ", obj_code=" + obj_code + ", obj_content=" + obj_content
				+ ", obj_detail_code=" + obj_detail_code + "]";
	}
	public AllObj_Dto(String main_obj, String obj_code, String obj_content, String obj_detail_code) {
		super();
		this.main_obj = main_obj;
		this.obj_code = obj_code;
		this.obj_content = obj_content;
		this.obj_detail_code = obj_detail_code;
	}
	
}