package ru.otus.homework.dao;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import ru.otus.homework.models.Account;
import ru.otus.homework.models.User;

public class AccountDao implements Dao<Account> {
    private final SessionFactory sessionFactory;

    public AccountDao(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public void create(Account objectData) {
        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.getTransaction();
            transaction.begin();
            session.save(objectData);
            transaction.commit();
        }
    }

    @Override
    public void update(Account objectData) {
        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.getTransaction();
            transaction.begin();
            session.update(objectData);
            transaction.commit();
        }
    }


    @Override
    public void delete(Account objectData) {
        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.getTransaction();
            transaction.begin();
            session.delete(objectData);
            transaction.commit();
        }
    }

    @Override
    public Account load(long id) {
        try (Session session = sessionFactory.openSession()) {
            return session.get(Account.class, id);
        }
    }
}