package com.connection;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

public class HibernateUtil {
	public static SessionFactory factory;
	public static Session session;
	
	public static SessionFactory getFactory() {
		try {
			factory = new Configuration().configure("hibernate.cfg.xml").buildSessionFactory();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return factory;
	}
	
	public static Session getSession() {
		try {
			session = HibernateUtil.getFactory().openSession();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return session;
	}
	public void closeFactory(){
		factory.close();
	}
	public void closeSession() {
		session.close();
	}
}