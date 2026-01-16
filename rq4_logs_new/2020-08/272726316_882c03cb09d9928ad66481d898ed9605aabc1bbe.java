package com.gslab.talent.model;

import java.util.List;

public class Helper {
	List<String> positionName;
	List<Integer> application;
	List<Integer> tech1;
	List<Integer> tech2;
	List<Integer> tech3;
	List<Integer> manager;
	List<Integer> hr;
	
	public Helper() {
		// TODO Auto-generated constructor stub
	}

	public List<String> getPositionName() {
		return positionName;
	}

	public void setPositionName(List<String> positionName) {
		this.positionName = positionName;
	}

	public List<Integer> getApplication() {
		return application;
	}

	public void setApplication(List<Integer> application) {
		this.application = application;
	}

	public List<Integer> getTech1() {
		return tech1;
	}

	public void setTech1(List<Integer> tech1) {
		this.tech1 = tech1;
	}

	public List<Integer> getTech2() {
		return tech2;
	}

	public void setTech2(List<Integer> tech2) {
		this.tech2 = tech2;
	}

	public List<Integer> getTech3() {
		return tech3;
	}

	public void setTech3(List<Integer> tech3) {
		this.tech3 = tech3;
	}

	public List<Integer> getManager() {
		return manager;
	}

	public void setManager(List<Integer> manager) {
		this.manager = manager;
	}

	public List<Integer> getHr() {
		return hr;
	}

	public void setHr(List<Integer> hr) {
		this.hr = hr;
	}

	public Helper(List<String> positionName, List<Integer> application, List<Integer> tech1, List<Integer> tech2,
			List<Integer> tech3, List<Integer> manager, List<Integer> hr) {
		super();
		this.positionName = positionName;
		this.application = application;
		this.tech1 = tech1;
		this.tech2 = tech2;
		this.tech3 = tech3;
		this.manager = manager;
		this.hr = hr;
	}

	@Override
	public String toString() {
		return "Helper [positionName=" + positionName + ", application=" + application + ", tech1=" + tech1 + ", tech2="
				+ tech2 + ", tech3=" + tech3 + ", manager=" + manager + ", hr=" + hr + "]";
	}
	
	

	
	


}