package com.sms.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.sms.entity.Supplies;
import com.sms.entity.SupplyTypes;

public interface SuppliesDAO {
	public List<SupplyTypes> getSupplyTypes() throws SQLException;
	public List<Supplies> getSupplies() throws SQLException;
	public void insertSupplies(Map <String, Object> supplies) throws SQLException;
	public void updateSupplies(Map <String, Object> supplies) throws SQLException;
	public List<Supplies> searchSupplies(String itemName) throws SQLException;
}