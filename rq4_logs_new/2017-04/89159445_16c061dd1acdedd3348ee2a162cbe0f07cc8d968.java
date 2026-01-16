package com.trips.config;

import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.view.InternalResourceViewResolver;

@Configuration
@ComponentScan("com.trips")
@EnableJpaRepositories(basePackages = "com.trips.repository",
	considerNestedRepositories = true
//	,
//	entityManagerFactoryRef = "entityManager",
//	transactionManagerRef = "transactionManager"
	)
@EnableTransactionManagement
@PropertySource("classpath:application.properties")
public class ApplicationContextConfig {

	@Autowired
	private Environment env;

	@Bean(name = "viewResolver")
	public InternalResourceViewResolver getViewResolver() {
		InternalResourceViewResolver viewResolver = new InternalResourceViewResolver();
		viewResolver.setPrefix("/WEB-INF/views/");
		viewResolver.setSuffix(".jsp");
		return viewResolver;
	}

	@Bean(name = "dataSource")
	public DataSource getDataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(env.getProperty("ds.database-driver"));
		dataSource.setUrl(env.getProperty("ds.url"));
		dataSource.setUsername(env.getProperty("ds.username"));
		dataSource.setPassword(env.getProperty("ds.password"));
		System.out.println("## dataSource: " + dataSource);
		return dataSource;
	}
	
	   @Autowired
	   @Bean(name = "sessionFactory")
	   public SessionFactory getSessionFactory(DataSource dataSource) throws Exception {
	       System.out.println("## getSessionFactory...");
	       try {
	           Properties properties = new Properties();
	           properties.put("hibernate.dialect", env.getProperty("hibernate.dialect"));
	           properties.put("hibernate.show_sql", env.getProperty("hibernate.show_sql"));
	           properties.put("current_session_context_class", env.getProperty("current_session_context_class"));
	           LocalSessionFactoryBean factoryBean = new LocalSessionFactoryBean();
	           factoryBean.setPackagesToScan(new String[] { "com.trips.entity" });
	           factoryBean.setDataSource(dataSource);
	           factoryBean.setHibernateProperties(properties);
	           factoryBean.afterPropertiesSet();
	           SessionFactory sf = factoryBean.getObject();
	           System.out.println("## getSessionFactory: " + sf);
	           return sf;
	       } catch (Exception e) {
	           System.out.println("Error getSessionFactory: " + e);
	           e.printStackTrace();
	           throw e;
	       }
	 
	   }
	 
//	   // Hibernate Transaction Manager
//	   @Autowired
//	   @Bean(name = "transactionManager")
//	   public HibernateTransactionManager getTransactionManager(SessionFactory sessionFactory) {
//	       HibernateTransactionManager transactionManager = new HibernateTransactionManager(sessionFactory);
//	       return transactionManager;
//	   }
	   
	   @Bean
	   public DataSource dataSource() {
	     EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
	     return builder.setType(EmbeddedDatabaseType.HSQL).build();
	   }
	   
	   @Bean
	   public EntityManagerFactory entityManagerFactory() {

	     HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
	     vendorAdapter.setGenerateDdl(true);

	     LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
	     factory.setJpaVendorAdapter(vendorAdapter);
	     factory.setPackagesToScan("com.trips.entity");
	     factory.setDataSource(getDataSource());
	     factory.afterPropertiesSet();

	     return factory.getObject();
	   }

	   @Bean
	   public PlatformTransactionManager transactionManager() {

	     JpaTransactionManager txManager = new JpaTransactionManager();
	     txManager.setEntityManagerFactory(entityManagerFactory());
	     return txManager;
	   }

}