package org.chuxue.application.dbms.tabs.vo;

import org.chuxue.application.bean.manager.dbms.SysDbmsTabsColsInfo;

import lombok.Getter;
import lombok.Setter;

/**
 * 文件名 ： SysDbmsTabsColsInfoVo.java
 * 包 名 ： org.chuxue.application.dbms.tabs.vo
 * 描 述 ： TODO(用一句话描述该文件做什么)
 * 机能名称：
 * 技能ID ：
 * 作 者 ： Administrator
 * 时 间 ： 2022年7月28日 上午10:22:34
 * 版 本 ： V1.0
 */
@Setter
@Getter
public class SysDbmsTabsColsInfoVo extends SysDbmsTabsColsInfo {
	
	// 表id
	private String tabsUuid;
	
	// 字段含义
	private String colsDesc;
	
	// 字段名
	private String colsName;
	
	// 字段长度
	private Long colsLength;
	
	// 数据不为空的百分比
	private Integer dataPrecision;
	
	// 用户查询列配置
	private String indexCode;
	
	// 数据类型（varchar,number,text） 相比字段类型更加规整
	private String dataType;
	
	// 字段类型（varchar,number,text）
	private String colsType;
	
	//
	private String nullable;
	
	// 数据的小数
	private Integer dataScale;
	
	// 默认为true显示该列，设为false则禁用列项目的选项卡。
	private Boolean colsSwitchable;
	
	// 对齐方式
	private String colsValign;
	
	// 用户查询显示图标
	private String userIcon;
	
	// 每列的宽度
	private Integer colsWidth;
	
	//
	private String colsDefault;
	
	// 对齐方式
	private String colsAlign;
}