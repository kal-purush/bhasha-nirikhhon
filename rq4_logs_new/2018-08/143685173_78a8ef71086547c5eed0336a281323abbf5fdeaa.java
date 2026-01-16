package com.sms.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.sms.entity.Supply;
import com.sms.entity.SupplyStock;

public interface SupplyStockDAO {
	List<SupplyStock> getSupplyStock() throws SQLException;
	List<Supply> getSupplyNames() throws SQLException;
	List<Supply> getAllSupplyNames() throws SQLException;
	void insertSupplyStock(Map<String, Object> params) throws SQLException;
	void updateAddedStock(Map<String, Object> params) throws SQLException;
	void updateSupplyStock(Map<String, Object> params) throws SQLException;
	List<SupplyStock> searchSupplyStock(Map<String, Object>search) throws SQLException;
	void updateSupplyQuantity(Map<String, Object> params) throws SQLException;
}