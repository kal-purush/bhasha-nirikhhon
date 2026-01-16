package com.future.dao;

import com.future.domain.User;

public interface UserDao {
     
	//根据登录名查询用户对象
	User getUserByCode(String code);
	
	//保存用户
	void save(User u);
}