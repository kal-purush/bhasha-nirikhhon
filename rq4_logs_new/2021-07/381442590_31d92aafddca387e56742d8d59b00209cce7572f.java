package com.upgrad.FoodOrderingApp.service.dao;

import com.upgrad.FoodOrderingApp.service.entity.CategoryEntity;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import java.util.List;

/**
 * @author zeelani
 * Entity class for performing the DB operations on Category table
 */

@Repository
public class CategoryDao {

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Method to retrive the category using the id
     * @param uuid
     * @return
     */
    public CategoryEntity fetchByUuid(String uuid) {
        try {
            CategoryEntity categoryEntity = entityManager.createNamedQuery("category.fetchByUuid",CategoryEntity.class).setParameter("uuid",uuid).getSingleResult();
            return categoryEntity;
        }catch (NoResultException nre){
            return null;
        }
    }

    /**
     * Method to list all the categories
     * @return
     */
    public List<CategoryEntity> fetchAll() {
        try {
            List<CategoryEntity> categoryEntities = entityManager.createNamedQuery("category.fetchAll",CategoryEntity.class).getResultList();
            return categoryEntities;
        }catch (NoResultException nre){
            return null;
        }
    }
}