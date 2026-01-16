package ru.otus.jdbc.mapper.impls;

import ru.otus.jdbc.DbExecutor;
import ru.otus.jdbc.mapper.*;
import ru.otus.jdbc.sessionmanager.SessionManagerJdbc;

public class DefaultJdbcMapperFactory implements JdbcMapperFactory {

    private final SessionManagerJdbc sessionManager;

    private final FactoryEntityMetaData factoryEntityMetaData;

    private final FactorySqlMetaData factorySqlMetaData;

    public DefaultJdbcMapperFactory(SessionManagerJdbc sessionManager,
                                    FactoryEntityMetaData factoryEntityMetaData,
                                    FactorySqlMetaData factorySqlMetaData) {
        this.sessionManager = sessionManager;
        this.factoryEntityMetaData = factoryEntityMetaData;
        this.factorySqlMetaData = factorySqlMetaData;
    }

    @Override
    public <T> JdbcMapper<T> create(Class<T> tClass, DbExecutor<T> dbExecutor, Adapter<T> adapter) {
        EntityClassMetaData<T> meta = factoryEntityMetaData.create(tClass);
        EntitySQLMetaData sqlMetaData = factorySqlMetaData.create(meta);
        return new JdbcMapperImpl<>(sessionManager, dbExecutor, sqlMetaData, meta, adapter);
    }
}