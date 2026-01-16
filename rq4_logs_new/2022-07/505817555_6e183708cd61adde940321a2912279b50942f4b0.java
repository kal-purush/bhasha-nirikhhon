package org.chuxue.application.bean.manager.appl;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.chuxue.application.common.base.BaseEntity;

import lombok.Getter;
import lombok.Setter;

/**
 * @文件名 SysApplDataTypeInfo.java
 * @包名 org.chuxue.application.dbms.appl.po
 * @描述 `sys_appl_data_type_info`的实体类
 * @时间 2022年07月20日 10:58:32
 * @author
 * @版本 V1.0
 */
@Setter
@Getter
@Entity
@Table(name = "`sys_appl_data_type_info`")
@NamedQuery(name = "SysApplDataTypeInfo.findAll", query = "SELECT s FROM SysApplDataTypeInfo s")
public class SysApplDataTypeInfo extends BaseEntity implements Serializable {
	private static final long	serialVersionUID	= 1L;
	
	//
	@Column(name = "appl_code")
	protected String			applCode;
	
	//
	@Column(name = "type_code")
	protected String			typeCode;
	
	@Column(name = "checkbox_type")
	protected String			checkboxType;
	
	/**
	 * @param string
	 * 构造方法：
	 * 描 述： 默认构造函数
	 * 参 数：
	 * 作 者 ：
	 * @throws
	 */
	public SysApplDataTypeInfo(String applCode) {
		this.applCode = applCode;
	}
	
	/**
	 * 构造方法：
	 * 描 述： TODO(这里用一句话描述这个方法的作用)
	 * 参 数：
	 * 作 者 ： Administrator
	 * @throws
	 */
	public SysApplDataTypeInfo() {
	}
	
}