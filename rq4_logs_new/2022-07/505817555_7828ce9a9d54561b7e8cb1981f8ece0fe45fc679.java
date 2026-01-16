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
 * @文件名 SysApplInfo.java
 * @包名 org.chuxue.application.bean.manager.appl.po
 * @描述 `sys_appl_info`的实体类
 * @时间 2022年07月18日 16:07:49
 * @author
 * @版本 V1.0
 */
@Setter
@Getter
@Entity
@Table(name = "`sys_appl_info`")
@NamedQuery(name = "SysApplInfo.findAll", query = "SELECT s FROM SysApplInfo s")
public class SysApplInfo extends BaseEntity implements Serializable {
	private static final long	serialVersionUID	= 1L;
	
	// 代码
	@Column(name = "appl_code")
	private String				applCode;
	
	// 名称
	@Column(name = "appl_name")
	private String				applName;
	
	/**
	 * 构造方法：
	 * 描 述： 默认构造函数
	 * 参 数：
	 * 作 者 ：
	 * @throws
	 */
	public SysApplInfo() {
	}
	
}