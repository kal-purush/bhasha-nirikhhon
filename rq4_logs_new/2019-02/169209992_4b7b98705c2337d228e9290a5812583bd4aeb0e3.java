package org.elaastic.qtapi.Services;

import org.elaastic.qtapi.entities.Assignment;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.transaction.Transactional;
import java.util.List;

@Service
@Transactional
public class AssignmentService {

    @PersistenceContext
    private EntityManager entityManager;

    public EntityManager getEntityManager() {

        return entityManager;
    }

    public void setEntityManager(EntityManager entityManager) {

        this.entityManager = entityManager;
    }

    public List<Assignment> findAllAssignments() {

        String query = "SELECT m FROM Assignment m ORDER BY m.title" ;
        TypedQuery<Assignment> queryObj = entityManager.createQuery(query, Assignment.class);
        return queryObj.getResultList();
    }

}