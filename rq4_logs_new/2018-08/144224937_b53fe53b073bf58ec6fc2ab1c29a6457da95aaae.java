package com.cpi.dao;

import java.sql.SQLException;
import java.util.Map;

import com.cpi.entity.Login;

public interface LoginDAO {
	
	public Login getUserInfo(Map<String, Object> userInfo) throws SQLException;
	public String checkUserId(String userId) throws SQLException;
	public Integer checkAttempts(String userId) throws SQLException;
	public void insertAttempt(String userId) throws SQLException;
	public void updateAttempt(String userId) throws SQLException;
	public void lockAccount(String userId) throws SQLException;
	public void updateLastLogin(String userId) throws SQLException;
	public void truncateAttempts() throws SQLException;
}