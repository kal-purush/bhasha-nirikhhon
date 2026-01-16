package com.xebia.jpaexcercises._03_lazy;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

public class LazyApp {

    public static void main(String[] args) {
        EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("jpa-examples-pu");
        EntityManager entityManager = entityManagerFactory.createEntityManager();

        entityManager.getTransaction().begin();
        Employee employee = new Employee();
        employee.setId(1L);
        employee.setName("Test Employee");
        employee.setBio("I love programming");
        entityManager.persist(employee);
        entityManager.getTransaction().commit();

        entityManager.close();

        entityManager = entityManagerFactory.createEntityManager();
        Employee foundEmployee = entityManager.find(Employee.class, 1L);

        System.out.println(foundEmployee);
        entityManager.close();
        entityManagerFactory.close();

    }
}