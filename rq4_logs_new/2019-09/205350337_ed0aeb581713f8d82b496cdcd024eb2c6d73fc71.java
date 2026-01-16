/**
 * Copyright (c) 2013-Now http://jeesite.com All rights reserved.
 */
package com.jeesite.modules.mallchannel.dao;

import com.jeesite.common.dao.CrudDao;
import com.jeesite.common.mybatis.annotation.MyBatisDao;
import com.jeesite.modules.mallchannel.entity.MallChannel;

/**
 * 产品分类DAO接口
 * @author nieyc
 * @version 2019-09-04
 */
@MyBatisDao
public interface MallChannelDao extends CrudDao<MallChannel> {
	
}