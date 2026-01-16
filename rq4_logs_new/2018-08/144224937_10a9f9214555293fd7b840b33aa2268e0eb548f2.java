package com.sms.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.sms.entity.SuppliesStock;
import com.sms.entity.Supply;


public interface SuppliesStocksDAO {
	List<SuppliesStock> getSuppliesStocks() throws SQLException;
	List<Supply> getSuppliesItemList() throws SQLException;
	void insertSuppliesStocks(Map<String, Object> params) throws SQLException;
	void updateSuppliesStocks(Map<String, Object> params) throws SQLException;
	void updateSupplies(Map<String, Object> params) throws SQLException;
	List<SuppliesStock> searchSuppliesStocks(Map<String, Object> params) throws SQLException;
}