package com.notable.data;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.notable.business.Order;

@Repository
public class OrdersJDBCTemplate implements OrdersDAO {

	private DataSource dataSource;
	
	@Autowired
	private JdbcTemplate jdbc;
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		this.jdbc = new JdbcTemplate(dataSource);
	}

	public void updateOrders(String email, double price, String status) {
		
		String innerSql = "SELECT userid FROM users WHERE email = '" + email + "'";
		String sql = "INSERT INTO Orders (UserID, Amount, Status) VALUES (("+ innerSql + "), ?, ?)";
		jdbc.update(sql, price, status);

	}

	public void updateOrderDetails(int productId, int quantity) {
		
		String innerSql = "SELECT orderid FROM orders ORDER BY orderid DESC LIMIT 1";
		
		String sql = "INSERT INTO OrderDetails (OrderID, ProductID, Quantity) VALUES (("+ innerSql + "), ?, ?)";
		jdbc.update(sql, productId, quantity);
		
	}

}