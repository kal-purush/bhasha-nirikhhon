package ru.itis.javawarrior.dao.impl;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.itis.javawarrior.dao.BaseDAO;
import ru.itis.javawarrior.entity.BaseEntity;

import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceContext;
import java.util.List;

@Transactional
@Repository
public class BaseDAOImpl implements BaseDAO {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public <T extends BaseEntity> T findById(Class<T> clazz, Long id) {
        return entityManager.unwrap(Session.class).get(clazz, id);
    }

    @Override
    public Long getCount(Class<?> clazz) {
        return (Long) entityManager.unwrap(Session.class).createQuery("SELECT count(o.id) FROM " + clazz.getName() + " o").uniqueResult();
    }

    @Override
    public Long save(BaseEntity o) {
        return (Long) entityManager.unwrap(Session.class).save(o);
    }

    @Override
    public <T extends BaseEntity> T delete(Class<T> clazz, Long id) {
        T o = findById(clazz, id);
        if (o != null) {
            entityManager.unwrap(Session.class).delete(o);
        } else {
            throw new EntityNotFoundException();
        }
        return o;
    }

    @Override
    public <T> List<T> getList(Class<T> clazz) {
        Criteria criteria = getCriteria(clazz);
        if (BaseEntity.class.isAssignableFrom(clazz)) {
            criteria.addOrder(Order.desc("id"));
        }
        return criteria.list();
    }

    private Criteria getCriteria(final Class<?> clazz) {
        return entityManager.unwrap(Session.class).createCriteria(clazz);
    }
}