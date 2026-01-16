package com.github.ssullivan.db.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ssullivan.RedisConfig;
import com.github.ssullivan.db.IServiceCodesDao;
import com.github.ssullivan.model.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ServicesDaoMockitoTest {

  private static final com.fasterxml.jackson.databind.ObjectMapper ObjectMapper = new ObjectMapper();
  private IRedisConnectionPool redisPool;
  private StatefulRedisConnection<String, String> redisConnection;
  private RedisCommands<String, String> redisCommand;
  private Injector injector;
  private IServiceCodesDao dao;
  private Service service;


  @BeforeEach()
  public void initEach() throws Exception {
    RedisConfig redisConfig = new RedisConfig();
    redisConfig.setTimeout(5L);
    redisPool = Mockito.mock(IRedisConnectionPool.class);
    redisConnection = (StatefulRedisConnection<String, String>) Mockito
        .mock(StatefulRedisConnection.class);
    redisCommand = (RedisCommands<String, String>) Mockito.mock(RedisCommands.class);

    Mockito.when(redisConnection.sync()).thenReturn(redisCommand);
    Mockito.when(redisPool.borrowConnection()).thenReturn(redisConnection);
    Mockito.when(redisPool.borrowConnection(Mockito.anyLong())).thenReturn(redisConnection);

    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(IRedisConnectionPool.class).toInstance(redisPool);
        bind(ObjectMapper.class).toInstance(ObjectMapper);
        bind(RedisConfig.class).toInstance(redisConfig);
      }
    });

    this.service = new Service();
    service.setCode("TEST");
    service.setName("A test service");
    service.setCategoryCode("TEST");
    service.setDescription("A test service");

    Mockito.when(redisCommand
        .hset(Mockito.eq(RedisServiceCodeDao.KEY), Mockito.eq("TEST"), Mockito.anyString()))
        .thenReturn(true);
    Mockito.when(redisCommand.hget(Mockito.eq(RedisServiceCodeDao.KEY), Mockito.eq("TEST")))
        .thenReturn(ObjectMapper.writeValueAsString(service));

    dao = injector.getInstance(IServiceCodesDao.class);
  }

  @Test
  public void addService() throws Exception {

    final boolean wasadded = dao.addService(service);

    MatcherAssert.assertThat(wasadded, Matchers.equalTo(true));

  }

  @Test
  public void testGetService() throws Exception {
    final boolean wasadded = dao.addService(service);
    final Service fromDb = dao.get(service.getCode());

    MatcherAssert.assertThat(fromDb.getCode(), Matchers.equalTo(service.getCode()));
    MatcherAssert.assertThat(fromDb.getName(), Matchers.equalTo(service.getName()));
    MatcherAssert.assertThat(fromDb.getCategoryCode(), Matchers.equalTo(service.getCategoryCode()));
    MatcherAssert.assertThat(fromDb.getDescription(), Matchers.equalTo(service.getDescription()));

  }
}