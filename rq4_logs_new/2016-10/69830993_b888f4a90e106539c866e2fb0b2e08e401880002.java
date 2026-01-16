package myDentist.model.dao.jpa;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import myDentist.model.User;
import myDentist.model.dao.userDao;

import org.springframework.stereotype.Repository;

@Repository
public class UserDaoImpl implements userDao {
	
	@PersistenceContext
	private EntityManager entitymanager;
	
	@Override
	public User getUser(Integer userId)
	{
		return entitymanager.find(User.class,userId);
	}
	
	@Override
	public List<User> getUsers()
	{
		return entitymanager.createQuery("from User order by userId", User.class).getResultList();
	}
	
}