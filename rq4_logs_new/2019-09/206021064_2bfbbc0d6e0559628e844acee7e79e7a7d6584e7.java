package com.xebia.jpaexcercises._13_element_collections;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ElementCollectionApp {

    public static void main(String[] args) {
        EntityManagerFactory entityManagerFactory = null;
        EntityManager entityManager = null;
        try {
            entityManagerFactory = Persistence.createEntityManagerFactory("jpa-examples-pu");
            entityManager = entityManagerFactory.createEntityManager();
            entityManager.getTransaction().begin();

            Employee employee = new Employee("Test Employee");
            employee.setItems(Arrays.asList(new Item("pen", BigDecimal.valueOf(100L)), new Item("notebook",BigDecimal.valueOf(200L))));
            Set<String> nickNames = new HashSet<>();
            nickNames.add("test1");
            nickNames.add("test2");
            nickNames.add("test3");
            employee.setNickNames(nickNames);
            entityManager.persist(employee);
            entityManager.getTransaction().commit();

            Employee foundEmployee = entityManager.find(Employee.class, employee.getId());
            System.out.println(foundEmployee);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);

        } finally {
            entityManager.close();
            entityManagerFactory.close();
        }

    }
}