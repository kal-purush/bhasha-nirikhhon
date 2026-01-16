/**
 * Copyright &copy; 2012-2016 <a href="https://github.com/thinkgem/jeesite">JeeSite</a> All rights reserved.
 */
package com.office.system.common.baseEntity;

import java.util.Date;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.office.system.modlues.sysMsgM.entity.SysOUser;
import com.office.system.modlues.sysMsgM.entity.User;

/**
 * 数据Entity类,搜索框上的条件，可能是日期，文本框等等，数据库不设置对应的字段
 * @author lzc
 * 
 */
public abstract class QuickDataEntity<T> {

	protected String datemin;//搜索的开始时间
	
	protected String datemax;//搜索的结束时间
	
	protected String conditionText;//输入框的内容，可能是文本框，字段可能是name，text，title等等

	

	public String getDatemin() {
		return datemin;
	}

	public void setDatemin(String datemin) {
		this.datemin = datemin;
	}

	public String getDatemax() {
		return datemax;
	}

	public void setDatemax(String datemax) {
		this.datemax = datemax;
	}

	public String getConditionText() {
		return conditionText;
	}

	public void setConditionText(String conditionText) {
		this.conditionText = conditionText;
	}


	


}