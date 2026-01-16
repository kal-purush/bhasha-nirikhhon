package main.java.com.util;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateUtility {
	private static final SessionFactory sessionFactory;
	static{
		try{
			sessionFactory = new Configuration().configure("hibernate.cfg.xml").buildSessionFactory(); 
		}catch(Throwable throwable){
			System.err.println("SessionFactory creation failed"+throwable);
			throw new ExceptionInInitializerError(throwable);
		}
	}
	
	public static synchronized SessionFactory getSessionFactory(){
		return sessionFactory;
	}

}