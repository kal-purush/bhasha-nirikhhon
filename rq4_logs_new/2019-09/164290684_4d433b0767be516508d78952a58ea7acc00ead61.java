package com.stupidwind.blog.config;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.context.annotation.Configuration;

import java.util.Date;

/**
 * @author: StupidWind
 * @date: 2019/9/9 20:23
 * @description: MetaObjectHandler配置类
 * @ver: 1.0.0
 */
@Configuration
public class MetaObjectHandlerConfiguration implements MetaObjectHandler {

	@Override
	public void insertFill(MetaObject metaObject) {
		this.setFieldValByName("create_time", new Date(), metaObject);
		this.setFieldValByName("update_time", new Date(), metaObject);
	}

	@Override
	public void updateFill(MetaObject metaObject) {
		this.setFieldValByName("update_time", new Date(), metaObject);
	}

}