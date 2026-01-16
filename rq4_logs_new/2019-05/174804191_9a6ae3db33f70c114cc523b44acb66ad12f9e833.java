package DAL.DAO;

import java.io.Serializable;
import java.util.List;

import org.hibernate.Query;
import utils.HibernateUtil;
import java.util.ArrayList;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractHibernateDAO <T extends Serializable> implements IOperations<T> {
    private Class<T> clazz;

    @Autowired
    private SessionFactory sessionFactory;

    protected final void setClazz(final Class<T> clazzToSet) {
        clazz = clazzToSet;
    }


    public final T getById(final int id) {
        Session session = HibernateUtil.getSessionFactory().openSession();
        session.beginTransaction();
        T result = (T) session.get(clazz, id);
        session.getTransaction().commit();
        if (session.isOpen()) {
            session.close();
        }
        return result;
    }


    public final List<T> getAll() {
        Session session = HibernateUtil.getSessionFactory().openSession();
        session.beginTransaction();
        String hql = String.format("from %s",clazz.getCanonicalName());
        Query SQLQuery = session.createQuery(hql);
        ArrayList<T> result = (ArrayList<T>) SQLQuery.list();
        session.getTransaction().commit();
        if (session.isOpen()) {
            session.close();
        }
        return result;
    }

    @Override
    public final void create(final T entity) {
        Session session = HibernateUtil.getSessionFactory().openSession();
        session.beginTransaction();
        session.save(entity);
        session.getTransaction().commit();
        if (session.isOpen()) {
            session.close();
        }
    }

    @Override
    public final T update(final T entity) {
        Session session = HibernateUtil.getSessionFactory().openSession();
        session.beginTransaction();
        session.merge(entity);
        session.getTransaction().commit();
        if (session.isOpen()) {
            session.close();
        }
        return (T) entity;
    }

    @Override
    public final void delete(final T entity) {

        getCurrentSession().delete(entity);
    }

    @Override
    public final void deleteById(final int entityId) {
        //final T entity = getById(entityId);
        Session session = HibernateUtil.getSessionFactory().openSession();
        session.beginTransaction();
        T del = (T) session.get(clazz, entityId);
        session.delete(del);
        session.getTransaction().commit();
        if (session.isOpen()) {
            session.close();
        }
       // Preconditions.checkState(entity != null);
       // delete(entity);
    }

    public final Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }

}