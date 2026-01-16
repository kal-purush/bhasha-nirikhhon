package com.github.ssullivan.db.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ssullivan.db.ICategoryCodesDao;
import com.github.ssullivan.model.Category;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.RedisCommand;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CategoryDaoMockitoTest {
    private static final ObjectMapper ObjectMapper = new ObjectMapper();
    private IRedisConnectionPool redisPool;
    private StatefulRedisConnection<String, String> redisConnection;
    private RedisCommands<String, String> redisCommand;
    private Injector injector;
    private ICategoryCodesDao dao;


    @BeforeEach()
    public void initEach() throws Exception {
        redisPool = Mockito.mock(IRedisConnectionPool.class);
        redisConnection = (StatefulRedisConnection<String, String>) Mockito.mock(StatefulRedisConnection.class);
        redisCommand = (RedisCommands<String, String>) Mockito.mock(RedisCommands.class);

        Mockito.when(redisConnection.sync()).thenReturn(redisCommand);
        Mockito.when(redisPool.borrowConnection()).thenReturn(redisConnection);
        Mockito.when(redisPool.borrowConnection(Mockito.anyLong())).thenReturn(redisConnection);

        injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(IRedisConnectionPool.class).toInstance(redisPool);
                bind(ObjectMapper.class).toInstance(ObjectMapper);
            }
        });

        dao = injector.getInstance(ICategoryCodesDao.class);
    }

    @Test
    public void addCategory() throws Exception {
        Mockito.when(redisCommand.hset(Mockito.anyString(), Mockito.eq("TEST"), Mockito.anyString()))
                .thenReturn(true);

        Category category = new Category();
        category.setCode("TEST");
        category.setName("A test category");
        category.setServiceCodes(ImmutableSet.of("FOO"));

        final boolean wasadded = dao.addCategory(category);
        MatcherAssert.assertThat(wasadded, Matchers.equalTo(true));
    }
}