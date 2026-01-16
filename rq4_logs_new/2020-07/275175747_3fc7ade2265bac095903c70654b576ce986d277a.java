package jm.dao;

import jm.User;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

public class AbstractDAO<T> {

    @PersistenceContext
    EntityManager entityManager;

    private Class clazz;

    public AbstractDAO() {
        clazz = (Class) ((java.lang.reflect.ParameterizedType)
                this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public List<T> getAll() {
        return entityManager.createQuery("FROM " + clazz.getName()).getResultList();
    }

    public T getById(Long id) {
        return (T) entityManager.find(clazz, id);
    }

    public void addUser(User user) {
        entityManager.persist(user);
    }

    public User merge(User user) {
        return entityManager.merge(user);
    }

    public void deleteById(Long id) {
        entityManager.remove(getById(id));
    }
}