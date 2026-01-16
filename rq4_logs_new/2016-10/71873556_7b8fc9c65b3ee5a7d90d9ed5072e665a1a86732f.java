package cn.noteblog.pagination;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.property.PropertyTokenizer;
import org.apache.ibatis.reflection.wrapper.ObjectWrapper;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.SimpleTypeRegistry;


import cn.noteblog.pagination.dialect.Dialect;
import cn.noteblog.pagination.domain.PageBounds;


@Intercepts({@Signature(type= Executor.class,method = "query",
		args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class})})  
public class PaginationInterceptor implements Interceptor{
	
	String dialectClass;
		
	@Override
	public Object intercept(Invocation invocation) throws Throwable {
		/**
		 * 此拦截器拦截的是Executor类的query方法,
		 */
		Executor executor = (Executor)invocation.getTarget();
		Object[] args = invocation.getArgs();
		MappedStatement mappedStatement = (MappedStatement)args[0];
		/*此对象是*.xml中的resultMap或resultType入参，可以看到此对象是Object类型的*/
		Object parameterObject = args[1];
		/*此对象是分页必传对象，此对象中有两个相对重要的属性limit和offset，这两个属性是分页语句中的参数*/
		PageBounds pageBounds = (PageBounds)args[2];
		/*	
		 * 下面开始处理分页sql语句和参数
		 * 可以看到此处的newParameterObject,其实可以理解为它和parameterObject对象的功能相同，只是表现形式不一样，至于为什么要弄个Map?举个例子
		 * ：如果你在*mapper接口中的方法findBlogs(blog,page);当我们传的是个Blog对象的时候，此处的parameterObject就是个Blog对象,当我们拼接
		 * 分页sql语句append(" limit ?,?"),我们依这种形式入参的时候我们就要设置参数，这时候就会发现Blog中没有limit和offset属性,这时候就需把Blog
		 * 中有效的字段和值放到map中,并且添加limit和offset,put("limit",0)和put("offset",10)。可以看到return中newParameterObject参数，
		 * 其实它就是query方法中的一个重要的参数，
		 */
		
		/*newparameterObject和parameterMapping需要在数据库方言Dialect类中拼装，Dialect类中有个setParameter方法，
		 * 需要把参数绑定到newParameterObject和parameterMapping中
		 * 
		 */
		Class<?> clazz = Class.forName(dialectClass);
		Constructor<?> constructor = clazz.getConstructor(Object.class,MappedStatement.class,PageBounds.class,Executor.class);
		Dialect dialect = (Dialect) constructor.newInstance(parameterObject, mappedStatement,pageBounds,executor);
		//Dialect dialect = new Dialect(parameterObject, mappedStatement,pageBounds,executor);
		dialect.getCount();
		MappedStatement newMappedStatements = dialect.buildMappedStatement();
		
        
		return invocation.getMethod().invoke(executor,
                new Object[] { newMappedStatements,dialect.getNewParameterObject(), pageBounds, args[3] });
	}

	@Override
	public Object plugin(Object target) {
		return Plugin.wrap(target, this);
	}

	@Override
	public void setProperties(Properties properties) {
		dialectClass = properties.getProperty("dialectClass");
	}
 



}