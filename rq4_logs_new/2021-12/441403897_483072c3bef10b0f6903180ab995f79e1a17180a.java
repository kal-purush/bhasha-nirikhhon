package com.spartenportal.bean;

/**
 * Bean class for Roles Properties
 * 
 * @author Sejal Shelke
 * @date 23/11/2021
 * @version 1.0
 *
 */

import java.util.List;

import com.spartenportal.entity.User;


public class RolesBean {

	// Bean properties
	private Integer roleId;
	private String roleName;
	private List<User> users;
	
	// Getters and Setters
	public Integer getRoleId() {
		return roleId;
	}
	public void setRoleId(Integer roleId) {
		this.roleId = roleId;
	}
	public String getRoleName() {
		return roleName;
	}
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
	public List<User> getUsers() {
		return users;
	}
	public void setUsers(List<User> users) {
		this.users = users;
	}
	
	// Default Constructor
	public RolesBean() {
		
	}
	
	
	// Parameterized Constructor
	public RolesBean(String roleName) {
		super();
		this.roleName = roleName;
	}
	
	
	public RolesBean(String roleName, List<User> users) {
		super();
		this.roleName = roleName;
		this.users = users;
	}
	
	// toString Representation
	@Override
	public String toString() {
		return "RolesBean [roleId=" + roleId + ", roleName=" + roleName + "]";
	}
	
	
}