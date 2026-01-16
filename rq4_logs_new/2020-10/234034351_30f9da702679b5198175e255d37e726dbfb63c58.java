package com.diluv.confluencia.database;

import java.sql.Timestamp;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Root;

import org.hibernate.Session;

import com.diluv.confluencia.database.record.ImagesEntity;
import com.diluv.confluencia.utils.DatabaseUtil;

public class MiscDatabase {

    public boolean existsImagesForRelease (Session session) {

        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaQuery<ImagesEntity> q = cb.createQuery(ImagesEntity.class);
        Root<ImagesEntity> entity = q.from(ImagesEntity.class);

        q.where(cb.isFalse(entity.get("released")));

        TypedQuery<ImagesEntity> query = session.createQuery(q);
        query.setMaxResults(1);
        return DatabaseUtil.findOne(query.getResultList()) != null;
    }

    public int updateAllImagesForRelease (Session session, Timestamp time) {

        CriteriaBuilder cb = session.getCriteriaBuilder();
        CriteriaUpdate<ImagesEntity> q = cb.createCriteriaUpdate(ImagesEntity.class);
        Root<ImagesEntity> entity = q.from(ImagesEntity.class);

        q.set(entity.get("released"), true);
        q.where(
            cb.isFalse(entity.get("released")),
            cb.lessThanOrEqualTo(entity.get("createdAt"), time)
        );

        return session.createQuery(q).executeUpdate();
    }
}