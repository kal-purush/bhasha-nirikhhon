package com.templetree.dao;

import com.templetree.dao.intf.LoginDaoIntf;
import com.templetree.model.Attributes;
import com.templetree.model.Role;
import com.templetree.model.User;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

/**
 * @author Lalith Mannur
 */

@Repository("loginDao")
public class LoginDao implements LoginDaoIntf {
    @Autowired
    private SessionFactory sessionFactory;

    public Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }

    @Override
    public User getUserCredentials(String username) {
        return (User) getCurrentSession().createCriteria(User.class)
                .add(Restrictions.eq("username", username))
                .uniqueResult();
    }

    @Override
    public User updateUser(User user) {
        user.setUpdatedDate(new Timestamp(new Date().getTime()));
        getCurrentSession().update(user);
        return getUserCredentials(user.getUsername());
    }

    @Override
    public User insertUser(User user) {
        getCurrentSession().save(user);
        return getUserCredentials(user.getUsername());
    }

    @Override
    public List<User> retrieveAllUsers() {
        return getCurrentSession().createCriteria(User.class).list();
    }

    @Override
    public void deleteUser(Integer id) {
        getCurrentSession().delete(this.getUserById(id));
    }

    @Override
    public User getUserById(Integer id) {
        return (User) getCurrentSession().get(User.class, id);
    }

    @Override
    public List<Role> getAllRoles() {

        return getCurrentSession().createCriteria(Attributes.class)
                .add(Restrictions.eq("type", "roles"))
                .setProjection(Projections.distinct(Projections.property("value")))
                .list();

    }
}