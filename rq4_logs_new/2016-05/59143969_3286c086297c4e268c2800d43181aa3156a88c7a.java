package com.bls.patronage.db.dao;

import com.bls.patronage.db.mapper.AuditableEntityMapper;
import com.bls.patronage.db.model.AuditableEntity;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapper;

import java.util.Date;
import java.util.UUID;

abstract public class AuditDAO {

    @SqlUpdate("update decks set createdAt = :date, modifiedAt = :date, createdBy = :userId, modifiedBy = :userId where id = :id")
    abstract void createDeckAudit(@Bind("table") String table, @Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @SqlUpdate("update decks set modifiedAt = :date, modifiedBy = :userId where id = :id")
    abstract void updateDeckAudit(@Bind("table") String table, @Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @RegisterMapper(AuditableEntityMapper.class)
    @SqlQuery("select createdAt, modifiedAt, createdBy, modifiedBy from decks where id = :id")
    public abstract AuditableEntity getDeckAuditFields(@Bind("id") UUID id);


    @SqlUpdate("update tips set createdAt = :date, modifiedAt = :date, createdBy = :userId, modifiedBy = :userId where id = :id")
    abstract void createTipAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @SqlUpdate("update tips set modifiedAt = :date, modifiedBy = :userId where id = :id")
    abstract void updateTipAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @RegisterMapper(AuditableEntityMapper.class)
    @SqlQuery("select createdAt, modifiedAt, createdBy, modifiedBy from tips where id = :id")
    public abstract AuditableEntity getTipAuditFields(@Bind("id") UUID id);


    @SqlUpdate("update flashcards set createdAt = :date, modifiedAt = :date, createdBy = :userId, modifiedBy = :userId where id = :id")
    abstract void createFlashcardAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @SqlUpdate("update flashcards set modifiedAt = :date, modifiedBy = :userId where id = :id")
    abstract void updateFlashcardAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @RegisterMapper(AuditableEntityMapper.class)
    @SqlQuery("select createdAt, modifiedAt, createdBy, modifiedBy from flashcards where id = :id")
    public abstract AuditableEntity getFlashcardAuditFields(@Bind("id") UUID id);


    @SqlUpdate("update results set createdAt = :date, modifiedAt = :date, createdBy = :userId, modifiedBy = :userId where flashcardId = :id")
    abstract void createResultAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @SqlUpdate("update results set modifiedAt = :date, modifiedBy = :userId where flashcardId = :id")
    abstract void updateResultAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @RegisterMapper(AuditableEntityMapper.class)
    @SqlQuery("select createdAt, modifiedAt, createdBy, modifiedBy from results where id = :id")
    public abstract AuditableEntity getResultAuditFields(@Bind("id") UUID id);


    @SqlUpdate("update users set createdAt = :date, modifiedAt = :date, createdBy = :userId, modifiedBy = :userId where flashcardId = :id")
    abstract void createUserAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @SqlUpdate("update user set modifiedAt = :date, modifiedBy = :userId where flashcardId = :id")
    abstract void updateUserAudit(@Bind("id") UUID id, @Bind("date") Date date, @Bind("userId") UUID userId);

    @RegisterMapper(AuditableEntityMapper.class)
    @SqlQuery("select createdAt, modifiedAt, createdBy, modifiedBy from users where id = :id")
    public abstract AuditableEntity getUserAuditFields(@Bind("id") UUID id);

}