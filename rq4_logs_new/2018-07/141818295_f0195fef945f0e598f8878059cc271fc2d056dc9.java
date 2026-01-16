package ru.itis.grant.dao.impl;

import org.springframework.stereotype.Repository;
import ru.itis.grant.dao.interfaces.ActivationKeyDao;
import ru.itis.grant.model.ActivationKey;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Repository
public class ActivationKeyDaoImpl implements ActivationKeyDao {

    @PersistenceContext
    EntityManager em;

    @Override
    public void addActivationKey(ActivationKey activationKey) {
        em.persist(activationKey);
    }

    @Override
    public ActivationKey getActivationKey(String key) {
        return em.find(ActivationKey.class, key);
    }

    @Override
    public void deleteUserActivationKeys(long userId) {
        em.createQuery("delete from ActivationKey ak where ak.user.id = :id")
                .setParameter("id", userId)
                .executeUpdate();
    }

    @Override
    public boolean verifySendingKeyPossibility(long userId) {
        return false;
    }

    @Override
    public boolean activationKeyExistence(String key) {
        return !em.createQuery("SELECT 1 FROM ActivationKey ak WHERE ak.key = :key")
                .setParameter("key", key)
                .setMaxResults(1)
                .getResultList()
                .isEmpty();
    }
}