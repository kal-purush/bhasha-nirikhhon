package com.sms.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.sms.entity.Departments;
import com.sms.entity.ItemNames;
import com.sms.entity.Supplies;
import com.sms.entity.TableView;

public interface StockSuppliesDao {
	List<TableView> getIssue() throws SQLException;

	List<Departments> getDepartments() throws SQLException;

	List<Supplies> getSupplies() throws SQLException;

	List<ItemNames> getItemName() throws SQLException;
	

	void insertSupplies(Map<String, Object> supplies) throws SQLException;

	void updateSupplies(Map<String, Object> supplies) throws SQLException;

	List<TableView> searchFor(HttpServletRequest request) throws SQLException;
}