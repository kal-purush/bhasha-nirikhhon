package com.sms.dao.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sms.dao.LoginDAO;
import com.ibatis.sqlmap.client.SqlMapClient;
import com.sms.entity.Login;

public class LoginDAOImpl implements LoginDAO{

	private SqlMapClient loginSqlMapClient;
		
	public SqlMapClient getLoginSqlMapClient() {
		return loginSqlMapClient;
	}
	
	public void setLoginSqlMapClient(SqlMapClient loginSqlMapClient) {
		this.loginSqlMapClient = loginSqlMapClient;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Login> getLogin() throws SQLException {
		// TODO Auto-generated method stub
		List<Login> listLogin = new ArrayList<>();
		try{
			listLogin = this.getLoginSqlMapClient().queryForList("getLogin");
		}catch(SQLException e){
			System.out.println(e.getMessage());
		}
		return listLogin;
	}

	@Override
	public void updateLogin(Map<String, Object> params) throws SQLException {
		// TODO Auto-generated method stub
		try{
			this.loginSqlMapClient.startTransaction();
			this.loginSqlMapClient.getCurrentConnection().setAutoCommit(false);
			this.loginSqlMapClient.startBatch();
			
			this.getLoginSqlMapClient().update("updateLogin", params);
			
			this.loginSqlMapClient.executeBatch();
			this.loginSqlMapClient.getCurrentConnection().commit();
		} catch(SQLException e){
			System.out.println(e.getMessage());
			this.getLoginSqlMapClient().getCurrentConnection().rollback();
		} finally{
			this.loginSqlMapClient.endTransaction();
		}
	}

	@Override
	public void updateAttempts(Map<String, Object> params) throws SQLException {
		// TODO Auto-generated method stub
		try{
			this.loginSqlMapClient.startTransaction();
			this.loginSqlMapClient.getCurrentConnection().setAutoCommit(false);
			this.loginSqlMapClient.startBatch();
			
			this.getLoginSqlMapClient().update("updateAttempts", params);
			
			this.loginSqlMapClient.executeBatch();
			this.loginSqlMapClient.getCurrentConnection().commit();
		} catch(SQLException e){
			System.out.println(e.getMessage());
			this.getLoginSqlMapClient().getCurrentConnection().rollback();
		} finally{
			this.loginSqlMapClient.endTransaction();
		}
	}

	@Override
	public void updateResetAttempts(Map<String, Object> params) throws SQLException {
		// TODO Auto-generated method stub
		
		try{
			this.loginSqlMapClient.startTransaction();
			this.loginSqlMapClient.getCurrentConnection().setAutoCommit(false);
			this.loginSqlMapClient.startBatch();
			
			this.getLoginSqlMapClient().update("updateResetAttempts", params);
			
			this.loginSqlMapClient.executeBatch();
			this.loginSqlMapClient.getCurrentConnection().commit();
		} catch(SQLException e){
			System.out.println(e.getMessage());
			this.getLoginSqlMapClient().getCurrentConnection().rollback();
		} finally{
			this.loginSqlMapClient.endTransaction();
		}
	}

	@Override
	public void updateResetAllAttempts() throws SQLException {
		// TODO Auto-generated method stub
		
		try{
			this.loginSqlMapClient.startTransaction();
			this.loginSqlMapClient.getCurrentConnection().setAutoCommit(false);
			this.loginSqlMapClient.startBatch();
			
			this.getLoginSqlMapClient().update("updateResetAllAttempts");
			
			this.loginSqlMapClient.executeBatch();
			this.loginSqlMapClient.getCurrentConnection().commit();
		} catch(SQLException e){
			System.out.println(e.getMessage());
			this.getLoginSqlMapClient().getCurrentConnection().rollback();
		} finally{
			this.loginSqlMapClient.endTransaction();
		}
		
	}

}