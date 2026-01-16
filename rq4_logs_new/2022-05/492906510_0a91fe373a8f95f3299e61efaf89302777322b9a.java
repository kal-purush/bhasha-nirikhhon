package org.example.dao;

import org.example.model.Actor;
import org.hibernate.SessionFactory;

public class ActorDao extends EntityDao<Actor> {
    public ActorDao(SessionFactory sessionFactory) {
        super(sessionFactory, Actor.class);
    }
}