package ru.otus.core.dao.impls;

import ru.otus.core.dao.Dao;
import ru.otus.core.sessionmanager.SessionManager;
import ru.otus.jdbc.mapper.JdbcMapper;

import java.util.Optional;

public abstract class AbstractDaoJdbcMapper<T, ID> implements Dao<T, ID> {

    private final JdbcMapper<T> jdbcMapper;

    protected AbstractDaoJdbcMapper(JdbcMapper<T> jdbcMapper) {
        this.jdbcMapper = jdbcMapper;
    }

    @Override
    public Optional<T> findById(ID id) {
        return Optional.ofNullable(jdbcMapper.findById(id, getEntityClass()));
    }

    @Override
    public ID insertEntity(T entity) {
        jdbcMapper.insert(entity);
        return getId(entity);
    }

    @Override
    public void updateEntity(T entity) {
        jdbcMapper.update(entity);
    }

    @Override
    public void insertOrUpdate(T entity) {
        jdbcMapper.insertOrUpdate(entity);
    }

    @Override
    public SessionManager getSessionManager() {
        return jdbcMapper.getSessionManager();
    }

    protected abstract ID getId(T entity);

    protected abstract Class<T> getEntityClass();

}