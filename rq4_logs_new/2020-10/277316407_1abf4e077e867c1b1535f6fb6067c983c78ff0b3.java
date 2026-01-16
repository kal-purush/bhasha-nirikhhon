package ru.otus.core.service;

import ru.otus.cachehw.HwCache;
import ru.otus.core.dao.Dao;

import java.util.Optional;

public abstract class CachedAbstractDaoDBService<T, ID> extends AbstractDaoDBService<T, ID> {

    private final HwCache<ID, T> cache;

    public CachedAbstractDaoDBService(Dao<T, ID> dao, HwCache<ID, T> cache) {
        super(dao);
        this.cache = cache;
    }

    @Override
    public Optional<T> getModel(ID id) {
        T t = cache.get(id);
        if (t != null) {
            return Optional.of(t);
        }

        Optional<T> model = super.getModel(id);
        model.ifPresent(m -> cache.put(id, m));
        return model;
    }

    @Override
    public ID save(T model) {
        ID id = super.save(model);
        cache.put(id, model);
        return id;
    }
}