package com.office.system.modlues.sysMsgM.entity;

import com.office.system.common.baseEntity.DataEntity;

public class SysODepartmentDispatch extends DataEntity<SysODepartmentDispatch>{
	
	private String reason;
	
	private SysOUser dispatchUser;
	
	private SysODepartment oldDepartment;
	
	private SysODepartment newDeparment;
	
	public SysODepartmentDispatch(){}
	
	public SysODepartmentDispatch(String id){
		
		this.id = id;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public SysOUser getDispatchUser() {
		return dispatchUser;
	}

	public void setDispatchUser(SysOUser dispatchUser) {
		this.dispatchUser = dispatchUser;
	}

	public SysODepartment getOldDepartment() {
		return oldDepartment;
	}

	public void setOldDepartment(SysODepartment oldDepartment) {
		this.oldDepartment = oldDepartment;
	}

	public SysODepartment getNewDeparment() {
		return newDeparment;
	}

	public void setNewDeparment(SysODepartment newDeparment) {
		this.newDeparment = newDeparment;
	}
	
	

}