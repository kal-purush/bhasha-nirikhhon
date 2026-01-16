package com.online.cinema.dao.impl;

import com.online.cinema.dao.OrderDao;
import com.online.cinema.entity.Order;
import com.online.cinema.entity.User;
import com.online.cinema.exceptions.DataProcessingException;
import com.online.cinema.lib.Dao;
import com.online.cinema.util.HibernateUtil;
import java.util.List;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.hibernate.Session;

@Dao
public class OrderDaoImpl extends GenericDaoImpl<Order> implements OrderDao {

    @Override
    public List<Order> getOrderHistory(User user) {
        try (Session session = HibernateUtil.getSessionFactory().openSession()) {
            CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
            CriteriaQuery<Order> query
                    = criteriaBuilder.createQuery(Order.class);
            Root<Order> root = query.from(Order.class);
            root.fetch("user", JoinType.LEFT);
            root.fetch("tickets", JoinType.INNER);
            Predicate idPredicate
                    = criteriaBuilder.equal(root.get("user"), user.getId());
            return session.createQuery(query.distinct(true).where(idPredicate)).getResultList();
        } catch (Exception e) {
            throw new DataProcessingException(
                    "There was an error retrieving orders of user with id "
                            + user.getId(), e);
        }
    }

    @Override
    public List<Order> getAll() {
        return super.getAll(Order.class);
    }

    @Override
    public Order get(Long id) {
        return super.get(Order.class, id);
    }
}