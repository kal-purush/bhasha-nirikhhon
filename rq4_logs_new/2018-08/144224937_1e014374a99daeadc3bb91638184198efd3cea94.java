package com.sms.dao;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.sms.entity.User;

public interface UserDAO {
	List<User> getUser() throws SQLException;
	List<User> searchUser(Map<String, Object> params) throws SQLException;
	List<User> getPassword(Map<String, Object> params) throws SQLException;
	void insertUser(Map<String, Object> params) throws SQLException;
	void updateUser(Map<String, Object> params) throws SQLException;
	void updatePassword(Map<String, Object> params) throws SQLException;
	void updateThisUser(Map<String, Object> params) throws SQLException;
}