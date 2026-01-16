package com.gys.base.dao;

import java.io.Reader;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class DB {
	private static SqlSessionFactory sqlSessionFactory;
	private static Reader reader;
	public SqlSession session;
	static{
		try {
			reader=Resources.getResourceAsReader("mybatis-config.xml");
			sqlSessionFactory=new SqlSessionFactoryBuilder().build(reader);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public SqlSessionFactory getFactory(){
		return sqlSessionFactory;
	}
}