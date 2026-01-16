package com.notable.data;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.notable.business.OrderDetails;

public class OrderDetailsMapper implements RowMapper<OrderDetails> {

	@Override
	public OrderDetails mapRow(ResultSet rs, int rowNum) throws SQLException {
		
		OrderDetails od = new OrderDetails();
		
		od.setOrderId(rs.getInt("OrderID"));
		od.setProductName(rs.getString("name"));
		od.setPrice(rs.getDouble("price"));
		od.setQuantity(rs.getInt("Quantity"));
		od.setProductTotal(rs.getDouble("ProductTotal"));
		od.setOrderTotal(rs.getDouble("OrderTotal"));

		return od;
	}

}