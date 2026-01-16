package com.sp.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;

public class BillsResponseDTO implements Serializable {

	private static final long serialVersionUID = 1L;
	private String create_time;// 制单时间
	private String order_number;
	private String tracking_number;// 追踪号
	private String ref_number;// 客户订单号
	private String switch_tracking_number;// 转单号
	private String channel_name_en;
	private String country_name_en;
	private String weight;// 实重
	private String charge_weight;// 计费重
	private String currency;
	private String sales_amount;// 销售总金额
	private String sales_formula;// 销售计费公式
	private String remark;

	@XmlAttribute(name = "create_time")
	public String getCreate_time() {
		return create_time;
	}

	@XmlAttribute(name = "order_number")
	public String getOrder_number() {
		return order_number;
	}

	@XmlAttribute(name = "tracking_number")
	public String getTracking_number() {
		return tracking_number;
	}

	@XmlAttribute(name = "ref_number")
	public String getRef_number() {
		return ref_number;
	}

	@XmlAttribute(name = "switch_tracking_number")
	public String getSwitch_tracking_number() {
		return switch_tracking_number;
	}

	@XmlAttribute(name = "channel_name_en")
	public String getChannel_name_en() {
		return channel_name_en;
	}

	@XmlAttribute(name = "country_name_en")
	public String getCountry_name_en() {
		return country_name_en;
	}

	@XmlAttribute(name = "weight")
	public String getWeight() {
		return weight;
	}

	@XmlAttribute(name = "charge_weight")
	public String getCharge_weight() {
		return charge_weight;
	}

	@XmlAttribute(name = "currency")
	public String getCurrency() {
		return currency;
	}

	@XmlAttribute(name = "sales_amount")
	public String getSales_amount() {
		return sales_amount;
	}

	@XmlAttribute(name = "sales_formula")
	public String getSales_formula() {
		return sales_formula;
	}

	@XmlAttribute(name = "remark")
	public String getRemark() {
		return remark;
	}

	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}

	public void setOrder_number(String order_number) {
		this.order_number = order_number;
	}

	public void setTracking_number(String tracking_number) {
		this.tracking_number = tracking_number;
	}

	public void setRef_number(String ref_number) {
		this.ref_number = ref_number;
	}

	public void setSwitch_tracking_number(String switch_tracking_number) {
		this.switch_tracking_number = switch_tracking_number;
	}

	public void setChannel_name_en(String channel_name_en) {
		this.channel_name_en = channel_name_en;
	}

	public void setCountry_name_en(String country_name_en) {
		this.country_name_en = country_name_en;
	}

	public void setWeight(String weight) {
		this.weight = weight;
	}

	public void setCharge_weight(String charge_weight) {
		this.charge_weight = charge_weight;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public void setSales_amount(String sales_amount) {
		this.sales_amount = sales_amount;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public void setSales_formula(String sales_formula) {
		this.sales_formula = sales_formula;
	}

}