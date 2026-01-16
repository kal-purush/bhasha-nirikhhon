package ru.otus.lyamin.app.dao;

import org.hibernate.SessionFactory;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;

public class QueryCounter {
    private final SessionFactory sessionFactory;
    private static final long EXPECTED_QUERIES_COUNT = 1;

    public QueryCounter(TestEntityManager em) {
        this.sessionFactory = em.getEntityManager().getEntityManagerFactory().unwrap(SessionFactory.class);
        this.sessionFactory.getStatistics().clear();
        this.sessionFactory.getStatistics().setStatisticsEnabled(true);
    }

    public long getQueriesCount() {
        return sessionFactory.getStatistics().getPrepareStatementCount();
    }

    public static long getExpectedQueriesCount() {
        return EXPECTED_QUERIES_COUNT;
    }
}