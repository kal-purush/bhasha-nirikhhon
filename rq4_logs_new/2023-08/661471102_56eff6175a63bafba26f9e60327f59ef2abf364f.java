package com.intheeast;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.intheeast.springframe.dao.UserDao;
import com.mysql.cj.jdbc.Driver;

@Configuration
@EnableTransactionManagement
//@ComponentScan(basePackages = "com.intheeast.springframe.dao")
@ComponentScan(basePackages = "com.intheeast.springframe")
@Import(SqlServiceContext.class)
public class AppContext {
	@Bean
	public DataSource dataSource() {
		SimpleDriverDataSource ds = new SimpleDriverDataSource();
		ds.setDriverClass(Driver.class);
		ds.setUrl("jdbc:mysql://localhost:3306/testdb?characterEncoding=UTF-8");
		ds.setUsername("root");
		ds.setPassword("1234");
		return ds;
	}
	
	@Bean
	public PlatformTransactionManager transactionManager() {
		DataSourceTransactionManager tm = new DataSourceTransactionManager();
		tm.setDataSource(dataSource());
		return tm;
	}
	
	/**
	 * 애플리케이션 로직 & 테스트용 빈
	 */
	
	//@Autowired UserDao userDao;	
}