package marcel.IncubatorManagerFx.dao;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.hibernate.query.Query;

import marcel.IncubatorManagerFx.entity.Incubator;
import marcel.IncubatorManagerFx.entity.User;

public class IncubatorHibernateDAO  extends GenericHibernateDAO {

	public List<Incubator> getUserIncubators(User user) {
		getCurrentSession().beginTransaction();

		List<Incubator> incubators =  user.getIncubators();
		
		getCurrentSession().close();
		return incubators;
	}
}