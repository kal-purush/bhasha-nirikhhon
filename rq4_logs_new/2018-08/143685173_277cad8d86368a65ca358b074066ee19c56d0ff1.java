package com.sms.dao.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.sms.dao.DepartmentsDAO;
import com.sms.entity.Department;

public class DepartmentsDAOImpl implements DepartmentsDAO{
	private static final Logger LOGGER = LogManager.getLogger(IssuedSuppliesDAOImpl.class.getName());
	
	private SqlMapClient deptSqlMapClient;
	
	public SqlMapClient getDeptSqlMapClient() {
		return deptSqlMapClient;
	}
	
	public void setDeptSqlMapClient(SqlMapClient deptSqlMapClient) {
		this.deptSqlMapClient = deptSqlMapClient;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Department> getDepartments(String param) {
		List<Department> departments = new ArrayList<>();
		try{
			departments = deptSqlMapClient.queryForList("getDepartments", param);
			LOGGER.info("Departments have been provided.");
		} catch(SQLException e){
			LOGGER.error(e.getMessage());
		}
		return departments;
	}

}