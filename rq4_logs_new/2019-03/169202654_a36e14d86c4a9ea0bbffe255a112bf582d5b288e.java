package com.gcp.cart.model;

import java.util.Date;
import java.util.List;
import com.gcp.cart.domain.OrderItems;
import lombok.Data;

@Data
public class OrderDetail {
	private int orderId;
	private long memberId;
	private long addressId;
	private double totalPrice;
	private double totalShipping;
	private double totalTax;
	private double totalShippingTax;
	private String currency;
	private String status;
	private Date timePlaced;
	private Date timeUpdate;
	private double discount;
	private List<OrderItems> orderItems;	
}