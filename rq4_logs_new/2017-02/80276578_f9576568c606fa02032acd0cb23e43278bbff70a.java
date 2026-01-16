package org.zkoss.reference.developer.spring.security.dao;


import org.hibernate.Hibernate;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.zkoss.reference.developer.spring.security.model.Client;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.util.List;

@Repository
public class ClientDao {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public List<Client> getAll() {
        Query query = entityManager.createQuery("select c from Client as c");
        List<Client> result = query.getResultList();
        return result;
    }

    @Transactional( propagation = Propagation.REQUIRES_NEW)
    public Client insertClient(Client client) {
        entityManager.persist(client);
        entityManager.merge(client);
        return client;
    }

    @Transactional(readOnly = true)
    public Client findClientByName(String clientName) throws UsernameNotFoundException {
        Client client;
        try {
            Query query = entityManager.createQuery("select c from Client as c where c.name =:clientName")
                    .setParameter("clientName", clientName);
            client = (Client) query.getSingleResult();
            if (client != null) {
                Hibernate.initialize(client);
            }
        } catch (Exception e) {
            return null;
        }
        return client;
    }
}