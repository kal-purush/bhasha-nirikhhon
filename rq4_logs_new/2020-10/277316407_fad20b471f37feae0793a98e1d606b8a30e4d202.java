package ru.otus;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.otus.core.dao.AccountDao;
import ru.otus.core.dao.UserDao;
import ru.otus.core.dao.impls.jdbc.AccountDaoJdbcMapper;
import ru.otus.core.dao.impls.jdbc.UserDaoJdbcMapper;
import ru.otus.core.model.Account;
import ru.otus.core.model.User;
import ru.otus.core.service.AccountDaoDBService;
import ru.otus.core.service.UserDaoDBService;
import ru.otus.h2.DataSourceH2;
import ru.otus.jdbc.DbExecutorImpl;
import ru.otus.jdbc.mapper.*;
import ru.otus.jdbc.mapper.impls.*;
import ru.otus.jdbc.sessionmanager.SessionManagerJdbc;

import javax.sql.DataSource;
import java.util.Optional;


public class HomeWork {
    private static final Logger logger = LoggerFactory.getLogger(HomeWork.class);

    public static void main(String[] args) {
// Общая часть
        var dataSource = new DataSourceH2();
        flywayMigrations(dataSource);
        var sessionManager = new SessionManagerJdbc(dataSource);

//фабрики
        EntityMetaDataFactory entityMetaDataFactory = new EntityMetaDataFactoryImpl();
        SqlMetaDataFactory sqlMetaDataFactory = new SqlMetaDataFactoryImpl();
        JdbcMapperFactory jdbcMapperFactory = new DefaultJdbcMapperFactory(sessionManager, entityMetaDataFactory, sqlMetaDataFactory);

// Работа с пользователем
        DbExecutorImpl<User> dbExecutorUser = new DbExecutorImpl<>();
        Adapter<User> userAdapter = new UserAdapter();
        JdbcMapper<User> jdbcMapperUser = jdbcMapperFactory.create(User.class, dbExecutorUser, userAdapter);
        UserDao userDao = new UserDaoJdbcMapper(jdbcMapperUser);

// Работа с аккаунтом
        DbExecutorImpl<Account> dbExecutorAccount = new DbExecutorImpl<>();
        Adapter<Account> accountAdapter = new AccountAdapter();
        JdbcMapper<Account> accountJdbcMapper = jdbcMapperFactory.create(Account.class, dbExecutorAccount, accountAdapter);
        AccountDao accountDao = new AccountDaoJdbcMapper(accountJdbcMapper);

// Код дальше должен остаться, т.е. userDao должен использоваться
        var dbServiceUser = new UserDaoDBService(userDao);
        var idUser = dbServiceUser.save(new User(0L, "new user", 17));
        Optional<User> user = dbServiceUser.getModel(idUser);

        user.ifPresentOrElse(
                crUser -> logger.info("created user, name:{}", crUser.getName()),
                () -> logger.info("user was not created")
        );
// Работа со счетом
        var accountService = new AccountDaoDBService(accountDao);
        var idAccount = accountService.save(new Account(0L, "simple", 56.9));
        Optional<Account> account = accountService.getModel(idAccount);

        account.ifPresentOrElse(
                crAccount -> logger.info("created account, type:{}", crAccount.getType()),
                () -> logger.info("account was not created")
        );

    }

    private static void flywayMigrations(DataSource dataSource) {
        logger.info("db migration started...");
        var flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:/db/migration")
                .load();
        flyway.migrate();
        logger.info("db migration finished.");
        logger.info("***");
    }
}