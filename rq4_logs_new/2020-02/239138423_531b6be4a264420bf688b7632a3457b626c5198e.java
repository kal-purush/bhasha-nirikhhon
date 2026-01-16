package io.github.mat3e.lang;

import io.github.mat3e.HibernateUtil;

import java.util.List;
import java.util.Optional;

public class LangRepository {

    List<Lang> findAll(){
        var session = HibernateUtil.getSessionFactory().openSession();
        var transaction = session.beginTransaction();

        var result = session.createQuery("from Lang", Lang.class).list();

        transaction.commit();
        session.close();
        return result;

    }

    public Optional<Lang> findById(Integer id) {
        var session = HibernateUtil.getSessionFactory().openSession();
        var transaction = session.beginTransaction();

        var result = session.get(Lang.class, id);

        transaction.commit();
        session.close();
        return Optional.ofNullable(result);
    }
}