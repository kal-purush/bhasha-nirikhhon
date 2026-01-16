package com.sms.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.sms.entity.User;

public interface UserDAO {
	List<User> loadRecord() throws SQLException;
	List<User> loadRecord(String userId) throws SQLException;
	void insertRecord(Map<String, Object> params) throws SQLException;
	void updateRecord(Map<String, Object> params) throws SQLException;
	
	void updateLogDate(Map<String, Object> params) throws SQLException;
	
	List<User> loadProfile(String userId) throws SQLException;
	void updateProfile(Map<String, Object> params) throws SQLException;
	void updatePassword(Map<String, Object> params) throws SQLException;;

}