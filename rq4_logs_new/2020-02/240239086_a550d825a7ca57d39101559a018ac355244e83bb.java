package com.airport.mgmt.dao;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.SessionFactory;

import com.airport.mgmt.form.User;

public class UserDaoImpl implements UserDao {

	private static Logger log=Logger.getLogger(UserDaoImpl.class);
	private SessionFactory sessionFactory;
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	@Override
	public void addUser(User user) {
		// TODO Auto-generated method stub
		log.info("Inside dao's addUser");
		sessionFactory.getCurrentSession().save(user);
	}

	@Override
	public void updateUser(User user) {
		// TODO Auto-generated method stub
		log.info("Inside dao's updateUser");
		sessionFactory.getCurrentSession().update(user);
	}

	@Override
	public List<User> listUser() {
		// TODO Auto-generated method stub
		Query q=sessionFactory.getCurrentSession().createQuery("from User");
		List l=q.list();
		return l;
	}

	@Override
	public User getUserById(int userId) {
		// TODO Auto-generated method stub
		Query q=sessionFactory.getCurrentSession().createQuery("from User u where u.id=:id");
		q.setParameter("id", userId);
		User user=(User)q.uniqueResult();
		return user;
	}

	@Override
	public User getUserByName(String userName) {
		// TODO Auto-generated method stub
		Query q=sessionFactory.getCurrentSession().createQuery("from User u where u.userName=:userName");
		q.setParameter("userName", userName);
		User user=(User)q.uniqueResult();
		return user;
	}
	
	@Override
	public void deleteUser(int userId) {
		// TODO Auto-generated method stub
		User user=(User)sessionFactory.getCurrentSession().get(User.class, userId);
		if(user != null)
		{
			sessionFactory.getCurrentSession().delete(user);
		}
	}
}