package dev.kyuelin.hibernate.dao;

import dev.kyuelin.hibernate.utils.HibernateUtil;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * Created by linken on 5/18/15.
 */
public class HibernateDao<T> {
//    public static SessionFactory getSessionFactory() {
//        return HibernateUtil.buildSessionFactory();
//    }
//
//    private T entity;
//    private Session session;
//
//    public void beginTransaction() {
//        session=HibernateDao.getSessionFactory().getCurrentSession();
//        session.beginTransaction();
//    }
//
//    public void commit() {
//        session.getTransaction().commit();
//    }
//
//    public HibernateDao(T entity) {
//        this.entity=entity;
//    }
//
//    public void update(T entity) {
//        session.update(entity);
//    }
//
//    public Long insert(T entity) {
//        return (Long) session.save(entity);
//    }
//
//    public void delete(T entity) {
//        session.delete(entity);
//    }
//
//    public T getByID(Long id) {
//        return (T) session.get(T, id);
//    }
}