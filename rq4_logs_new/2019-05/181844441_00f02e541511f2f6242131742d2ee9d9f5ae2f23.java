package com.executors;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;

import com.manytomany.EmployeeManyMany;
import com.manytomany.LaptopManyMany;


public class HibernateManyToManyExecutor {
	
	    private static SessionFactory buildSessionFactory() {
	        Configuration configuration = new Configuration();
	        configuration.configure("hibernate.cfg.xml");
	        ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder().
	        		applySettings(configuration.getProperties()).build();	 	          
	        return configuration.buildSessionFactory(serviceRegistry);
	    }

	public static void main(String[] args) {
		
		List<LaptopManyMany> laptops = new ArrayList<>();
		List<EmployeeManyMany> employs = new ArrayList<>();
		
		EmployeeManyMany emp1 = new EmployeeManyMany();
		emp1.setEmpName("Sarfaraz");
		
		EmployeeManyMany emp2 = new EmployeeManyMany();
		emp2.setEmpName("Areeb");
		
		
		LaptopManyMany lap1 = new LaptopManyMany();
		lap1.setLpName("Dell");
		
		LaptopManyMany lap2 = new LaptopManyMany();
		lap2.setLpName("HP");
		
		laptops.add(lap1);
		laptops.add(lap2);	
		employs.add(emp1);
		employs.add(emp2);
		
		
		emp1.setLaptop(laptops);
		emp2.setLaptop(laptops);
		
		lap1.setEmployee(employs);
		lap2.setEmployee(employs);
		
		Session session = buildSessionFactory().openSession();
		session.save(emp1);
		session.save(emp2);
		session.save(lap1);
		session.save(lap2);
		
		session.beginTransaction().commit();
		System.out.println("Transaction completed...");		
	}	
}