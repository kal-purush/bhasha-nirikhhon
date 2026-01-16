package com.order.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class OrderVO implements Serializable{
	private Integer orderID;
	private java.sql.Timestamp orderDate ;
	private String orderStaus;
	private Integer totalPrice;
	private Integer memberID;
	
	public OrderVO() {
		super();
		// TODO Auto-generated constructor stub
	}

	public OrderVO(Integer orderId, Timestamp orderDate, String orderStaus, Integer totalPrice, Integer memberId) {
		super();
		this.orderID = orderId;
		this.orderDate = orderDate;
		this.orderStaus = orderStaus;
		this.totalPrice = totalPrice;
		this.memberID = memberId;
	}

	public Integer getOrderId() {
		return orderID;
	}

	public void setOrderId(Integer orderId) {
		this.orderID = orderId;
	}

	public java.sql.Timestamp getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(java.sql.Timestamp orderDate) {
		this.orderDate = orderDate;
	}

	public String getOrderStaus() {
		return orderStaus;
	}

	public void setOrderStaus(String orderStaus) {
		this.orderStaus = orderStaus;
	}

	public Integer getTotalPrice() {
		return totalPrice;
	}

	public void setTotalPrice(Integer totalPrice) {
		this.totalPrice = totalPrice;
	}

	public Integer getMemberId() {
		return memberID;
	}

	public void setMemberId(Integer memberId) {
		this.memberID = memberId;
	}
	
	
	
	

}