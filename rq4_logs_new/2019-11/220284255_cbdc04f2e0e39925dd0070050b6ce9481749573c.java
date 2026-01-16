package com.revature.daos;

import org.hibernate.Session;
import org.hibernate.Transaction;

import com.revature.models.Location;
import com.revature.util.HibernateUtil;

public class LocationDao {

	public Location save(Location location) {
		try(Session sess = HibernateUtil.sessionFactory.openSession()) {
			sess.save(location);
			return location;
		}
	}

	public Location getById(int id) {
		try(Session sess = HibernateUtil.sessionFactory.openSession()) {
			return sess.get(Location.class, id);
		}
	}

	public Location update(Location location) {
		try(Session sess = HibernateUtil.sessionFactory.openSession()) {
			Transaction tx = sess.beginTransaction();
			sess.update(location);
			tx.commit();
			return location;
		}
	}

}