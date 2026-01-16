package com.wzq.tbmp.pojo;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name = "t_project")
@Cache(region = "tbmpHibernateCache", usage = CacheConcurrencyStrategy.READ_WRITE)
public class Project implements Serializable {
	private static final long serialVersionUID = 8891641747067064533L;
	private int projectId;
	private String projectName;
	private int process;
	private int device;
	private int warning;
	private int risk;
	private int allCircle;
	private int todayCircle;
	private String warningInfo;

	@Id
	@GenericGenerator(name = "generator", strategy = "increment")
	@GeneratedValue(generator = "generator")
	@Column(name = "project_id_")
	public int getProjectId() {
		return projectId;
	}

	public void setProjectId(int projectId) {
		this.projectId = projectId;
	}

	@Column(name = "project_name_")
	public String getProjectName() {
		return projectName;
	}

	public void setProjectName(String projectName) {
		this.projectName = projectName;
	}

	@Column(name = "process_")
	public int getProcess() {
		return process;
	}

	public void setProcess(int process) {
		this.process = process;
	}

	@Column(name = "device_")
	public int getDevice() {
		return device;
	}

	public void setDevice(int device) {
		this.device = device;
	}

	@Column(name = "warning_")
	public int getWarning() {
		return warning;
	}

	public void setWarning(int warning) {
		this.warning = warning;
	}

	@Column(name = "risk_")
	public int getRisk() {
		return risk;
	}

	public void setRisk(int risk) {
		this.risk = risk;
	}
	
	@Column(name = "all_circle_")
	public int getAllCircle() {
		return allCircle;
	}

	public void setAllCircle(int allCircle) {
		this.allCircle = allCircle;
	}

	@Column(name = "today_circle_")
	public int getTodayCircle() {
		return todayCircle;
	}

	public void setTodayCircle(int todayCircle) {
		this.todayCircle = todayCircle;
	}

	@Column(name = "warning_info_")
	public String getWarningInfo() {
		return warningInfo;
	}

	public void setWarningInfo(String warningInfo) {
		this.warningInfo = warningInfo;
	}
}