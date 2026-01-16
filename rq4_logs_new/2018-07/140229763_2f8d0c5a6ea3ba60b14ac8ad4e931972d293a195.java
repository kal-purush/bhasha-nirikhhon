/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import myentity.Color;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import myentity.Car;

/**
 *
 * @author midou
 */
public class MainTest {
    private EntityManagerFactory emf;
    private EntityManager em;
    
    public MainTest()
    {
        emf = Persistence.createEntityManagerFactory("Assignment2PU");
        em = emf.createEntityManager();
    }
    
    public void start() {
        if (emf != null) {
            end();
        }
        emf = Persistence.createEntityManagerFactory("Assignment2PU");
        em = emf.createEntityManager();
    }
    
    public void end() {
        if (emf != null) {
            em.close();
            emf.close();
            emf = null;
        }
    }
    
    public void save(Car car) {
        EntityTransaction tx = em.getTransaction();
        tx.begin();
        em.persist(car);
        tx.commit();
    }
    
    public static void main(String[] arg)
    {
        MainTest mt = new MainTest();
        Car c = new Car();
        Car c2 = new Car();
        Car c3 = new Car();
        Car c4 = new Car();
        Car c5 = new Car();
        
        c.setMake("Nissan");
        c.setModel("Murano");
        c.setYear(2018);
        c.setMiles(30000);
        c.setColor(Color.ORANGE);
        
        c2.setMake("Nissan");
        c2.setModel("Pathfinder");
        c2.setYear(2017);
        c2.setMiles(20000);
        c2.setColor(Color.WHITE);
        
        c3.setMake("Subaru");
        c3.setModel("Outback");
        c3.setYear(2018);
        c3.setMiles(10000);
        c3.setColor(Color.GREEN);
        
        c4.setMake("Ferrari");
        c4.setModel("488 Spider");
        c4.setYear(2018);
        c4.setMiles(4000);
        c4.setColor(Color.BLACK);
        
        c5.setMake("Lamborghini");
        c5.setModel("Huracan");
        c5.setYear(2018);
        c5.setMiles(3000);
        c5.setColor(Color.YELLOW);
        
        mt.save(c);
        mt.save(c2);
        mt.save(c3);
        mt.save(c4);
        mt.save(c5);
        mt.end();                
    }
    
}