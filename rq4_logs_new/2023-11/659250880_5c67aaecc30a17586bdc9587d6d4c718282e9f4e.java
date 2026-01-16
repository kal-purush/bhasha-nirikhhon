package com.fs.model;

public class ProductVO {
	
	private String m_id;
	private String p_id;
	private String p_name;
	private String p_type;
	private String p_category;
	private String p_enterprise;
	private String p_horiozntal;
	private String p_height;
	private String p_volume;
	private String p_quanity;
	
	
	public String getM_id() {
		return m_id;
	}
	public void setM_id(String m_id) {
		this.m_id = m_id;
	}
	public String getP_id() {
		return p_id;
	}
	public void setP_id(String p_id) {
		this.p_id = p_id;
	}
	public String getP_name() {
		return p_name;
	}
	public void setP_name(String p_name) {
		this.p_name = p_name;
	}
	public String getP_type() {
		return p_type;
	}
	public void setP_type(String p_type) {
		this.p_type = p_type;
	}
	public String getP_category() {
		return p_category;
	}
	public void setP_category(String p_category) {
		this.p_category = p_category;
	}
	public String getP_enterprise() {
		return p_enterprise;
	}
	public void setP_enterprise(String p_enterprise) {
		this.p_enterprise = p_enterprise;
	}
	public String getP_horiozntal() {
		return p_horiozntal;
	}
	public void setP_horiozntal(String p_horiozntal) {
		this.p_horiozntal = p_horiozntal;
	}
	public String getP_height() {
		return p_height;
	}
	public void setP_height(String p_height) {
		this.p_height = p_height;
	}
	public String getP_volume() {
		return p_volume;
	}
	public void setP_volume(String p_volume) {
		this.p_volume = p_volume;
	}
	public String getP_quanity() {
		return p_quanity;
	}
	public void setP_quanity(String p_quanity) {
		this.p_quanity = p_quanity;
	}
	
	
	
	@Override
	public String toString() {
		return "ProductVO [m_id=" + m_id + ", p_id=" + p_id + ", p_name=" + p_name + ", p_type=" + p_type
				+ ", p_category=" + p_category + ", p_enterprise=" + p_enterprise + ", p_horiozntal=" + p_horiozntal
				+ ", p_height=" + p_height + ", p_volume=" + p_volume + ", p_quanity=" + p_quanity + "]";
	}
	
	
	
}