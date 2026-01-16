package ru.otus.core.service;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.otus.cachehw.MyCache;
import ru.otus.core.model.Address;
import ru.otus.core.model.Phone;
import ru.otus.core.model.User;
import ru.otus.hibernate.DefaultSessionFactoryCreator;
import ru.otus.hibernate.HibernateFlywayMigrationManager;
import ru.otus.hibernate.HibernateSessionManager;
import ru.otus.hibernate.HibernateUserDao;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class CachedUserDaoDBServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(CachedUserDaoDBServiceTest.class);

    private static final String HIBERNATE_CONFIG_FILE = "hibernate.cfg.xml";

    private SessionFactory sessionFactory;

    private HibernateSessionManager hibernateSessionManager;

    private HibernateUserDao hibernateUserDao;

    private static final int limit = 100;

    @BeforeEach
    public void setUp() {
        var configuration = new Configuration().configure(HIBERNATE_CONFIG_FILE);

        HibernateFlywayMigrationManager migrationManager = new HibernateFlywayMigrationManager(configuration);
        migrationManager.migrate();

        var sessionFactoryCreator = new DefaultSessionFactoryCreator(configuration);
        sessionFactoryCreator.addEntityClasses(User.class, Address.class, Phone.class);

        sessionFactory = sessionFactoryCreator.create();
        hibernateSessionManager = new HibernateSessionManager(sessionFactory);
        hibernateUserDao = new HibernateUserDao(hibernateSessionManager);
    }

    @Test
    public void testWithCache() throws InterruptedException {
        MyCache<Long, User> cache = new MyCache<>();
        CachedUserDaoDBService service = new CachedUserDaoDBService(hibernateUserDao, cache);
        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < limit; i++) {
            Long id = service.save(createUser(i));
            ids.add(id);
        }
        long start = System.currentTimeMillis();
        ids.forEach(service::getModel);
        long end = System.currentTimeMillis();

        logger.info("time: {}", end - start);
        logger.info("cache size: {}", cache.size());
        System.gc();
        Thread.sleep(100);
        logger.info("cache size after gc: {}", cache.size());
    }

    @Test
    public void testWithoutCache() {
        UserDaoDBService service = new UserDaoDBService(hibernateUserDao);
        Set<Long> ids = new HashSet<>();
        for (int i = 0; i < limit; i++) {
            Long id = service.save(createUser(i));
            ids.add(id);
        }
        long start = System.currentTimeMillis();
        ids.forEach(service::getModel);
        long end = System.currentTimeMillis();
        logger.info("time: {}", end - start);
    }

    private User createUser(int index) {
        User user = new User(null, "user " + index, index);
        user.addPhones(Arrays.asList(
                new Phone(user, String.valueOf(index)),
                new Phone(user, String.valueOf(index + 1))
        ));
        user.setAddress(new Address(user, "street " + index));
        return user;
    }

}