package com.sms.dao.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.ibatis.sqlmap.client.SqlMapClient;
import com.sms.dao.SupplyDAO;
import com.sms.entity.Supply;

public class SupplyDAOImpl implements SupplyDAO{
	
	private SqlMapClient sqlMapClient;
	
	public SqlMapClient getSqlMapClient(){
		return sqlMapClient;
	}
	
	public void setSqlMapClient(SqlMapClient sqlMapClient) {
		this.sqlMapClient = sqlMapClient;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Supply> getSupplies() throws SQLException {
		List<Supply> listSup = new ArrayList<>();
		try{
			listSup = this.getSqlMapClient().queryForList("getSupplies");
		} catch (SQLException e){
			System.out.println(e.getMessage());
		}
		
		return listSup;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Supply> getSupplyTypes() throws SQLException {
		List<Supply> listSupType = new ArrayList<>();
		try{
			listSupType = this.getSqlMapClient().queryForList("getSupplyTypes");
		} catch (SQLException e){
			System.out.println(e.getMessage());
		}
		
		return listSupType;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Supply> searchSupply(String searchKey) throws SQLException {
		List<Supply> listSupSearch = new ArrayList<>();
		Map<String, Object> params = new HashMap<>();
//		try{
//			this.sqlMapClient.startTransaction();
//			this.sqlMapClient.getCurrentConnection().setAutoCommit(false);
//			this.sqlMapClient.startBatch();
//			
//			listSupSearch = this.getSqlMapClient().queryForList("getSupplySearch",searchKey);
//			this.sqlMapClient.executeBatch();
//			this.sqlMapClient.getCurrentConnection().commit();
//		} catch(SQLException e){
//			System.out.println(e.getMessage());
//			this.getSqlMapClient().getCurrentConnection().rollback();
//		} finally{
//			this.sqlMapClient.endTransaction();
//		}
		try{
			params.put("searchKey", searchKey);
			listSupSearch = this.getSqlMapClient().queryForList("getSupplySearch",params);
		} catch (SQLException e){
			System.out.println(e.getMessage());
		}
		System.out.println("dao");
		System.out.println(searchKey);
		return listSupSearch;
	}

	@Override
	public void insertSupply(Map<String, Object> params) throws SQLException {
		try {
			this.sqlMapClient.startTransaction();
			this.sqlMapClient.getCurrentConnection().setAutoCommit(false);
			this.sqlMapClient.startBatch();

			this.getSqlMapClient().update("insertSupply", params);

			this.sqlMapClient.executeBatch();
			this.sqlMapClient.getCurrentConnection().commit();

		} catch (SQLException e) {
			System.out.println(e.getLocalizedMessage());
			this.sqlMapClient.getCurrentConnection().rollback();
		} finally {
			this.sqlMapClient.endTransaction();
		}
		
	}

	@Override
	public void updateSupply(Map<String, Object> params) throws SQLException {
		try{
			this.sqlMapClient.startTransaction();
			this.sqlMapClient.getCurrentConnection().setAutoCommit(false);
			this.sqlMapClient.startBatch();
			this.getSqlMapClient().update("updateSupply", params);
			
			this.sqlMapClient.executeBatch();
			this.sqlMapClient.getCurrentConnection().commit();
		} catch(SQLException e){
			System.out.println(e.getMessage());
			this.getSqlMapClient().getCurrentConnection().rollback();
		} finally{
			this.sqlMapClient.endTransaction();
		}
		
	}

	

	

}