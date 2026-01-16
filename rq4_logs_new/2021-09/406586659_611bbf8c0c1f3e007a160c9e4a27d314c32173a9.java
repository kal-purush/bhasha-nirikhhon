package com.se.dao;

import java.sql.SQLException;
import java.util.ArrayList;

import com.se.vo.Customer;

public interface CustomerDao {
	public ArrayList<Customer> Cangku2() throws SQLException;
}