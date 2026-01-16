package ru.otus.hibernate;

import org.hibernate.Session;
import ru.otus.core.dao.Dao;
import ru.otus.core.model.HibernateModel;
import ru.otus.hibernate.exceptions.NotFoundEntityException;
import ru.otus.hibernate.exceptions.NotNullIdException;
import ru.otus.hibernate.exceptions.NullIdException;
import ru.otus.core.sessionmanager.SessionManager;

import java.io.Serializable;
import java.util.Optional;

public abstract class AbstractHibernateDao<T extends HibernateModel<ID>, ID extends Serializable> implements Dao<T, ID> {

    private final HibernateSessionManager hibernateSessionManager;

    public AbstractHibernateDao(HibernateSessionManager hibernateSessionManager) {
        this.hibernateSessionManager = hibernateSessionManager;
    }

    @Override
    public Optional<T> findById(ID id) {
        T entity = getCurrentSession().find(getClassEntity(), id);
        return Optional.ofNullable(entity);
    }

    @Override
    public ID insertEntity(T entity) {
        if (!isIdNull(entity)) {
            throw new NotNullIdException();
        }
        Session session = getCurrentSession();
        session.persist(entity);
        session.flush();
        return entity.getId();
    }

    @Override
    public void updateEntity(T entity) {
        if (isIdNull(entity)) {
            throw new NullIdException();
        }
        Session session = getCurrentSession();
        T t = session.get(getClassEntity(), entity.getId());
        if (t == null) {
            throw new NotFoundEntityException();
        }
        session.merge(entity);
        session.flush();
    }

    @Override
    public void insertOrUpdate(T entity) {
        if (isIdNull(entity)) {
            insertEntity(entity);
        } else {
            updateEntity(entity);
        }
    }

    @Override
    public SessionManager getSessionManager() {
        return getHibernateSessionManager();
    }

    protected Session getCurrentSession() {
        return hibernateSessionManager.getCurrentSession().getSession();
    }

    protected boolean isIdNull(T entity) {
        return entity.getId() == null;
    }

    protected HibernateSessionManager getHibernateSessionManager() {
        return hibernateSessionManager;
    }


    abstract protected Class<T> getClassEntity();

}