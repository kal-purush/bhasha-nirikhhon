package org.elsys.ip.repository;

import org.elsys.ip.config.HibernateUtil;
import org.elsys.ip.models.User;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.List;

public class UserRepository {
    public List<User> getUsers() {
        Session session = HibernateUtil.getSessionFactory().openSession();
        List<User> users = session.createQuery("from User").list();
        session.close();
        return users;
    }

    public User getUserById(Integer id) {
        Session session = HibernateUtil.getSessionFactory().openSession();
        User user = session.get(User.class, id);
        session.close();
        return user;
    }

    public void saveUser(User user) {
        Session session = HibernateUtil.getSessionFactory().openSession();;
        Transaction tx  = session.beginTransaction();
        session.save("User", user);
        tx.commit();
        session.close();
    }

    public User updateUser(Integer id, User user) {
        Session session = HibernateUtil.getSessionFactory().openSession();
        Transaction tx = session.beginTransaction();
        User persistentUser = getUserById(id);
        session.evict(persistentUser);
        persistentUser.update(user);
        User updatedUser = (User) session.merge(persistentUser);
        tx.commit();
        session.close();
        return updatedUser;
    }

    public void deleteUser(Integer id) {
        Session session = HibernateUtil.getSessionFactory().openSession();
        Transaction tx = session.beginTransaction();
        User persistentUser = getUserById(id);
        session.delete(persistentUser);
        tx.commit();
        session.close();
    }
}