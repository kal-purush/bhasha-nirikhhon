package com.home.service;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;

public class EmployeeService {
    private EntityManager entityManager;

    public EmployeeService() {
        entityManager = Persistence.createEntityManagerFactory("employee").createEntityManager();
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }
}