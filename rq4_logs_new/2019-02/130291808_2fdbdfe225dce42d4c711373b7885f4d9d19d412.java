package com.github.ssullivan.db.redis.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ssullivan.RedisConfig;
import com.github.ssullivan.db.ICategoryCodesDao;
import com.github.ssullivan.db.IFacilityDao;
import com.github.ssullivan.db.IFeedDao;
import com.github.ssullivan.db.IServiceCodesDao;
import com.github.ssullivan.db.redis.IRedisConnectionPool;
import com.github.ssullivan.utils.ShortUuid;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.mockito.Mockito;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractRedisIndexTest {

  private static final com.fasterxml.jackson.databind.ObjectMapper ObjectMapper = new ObjectMapper();

  protected static final String LocationId = ShortUuid.randomShortUuid();
  protected static final String FeedId = ShortUuid.randomShortUuid();

  private IRedisConnectionPool redisPool;
  private StatefulRedisConnection<String, String> redisConnection;
  private RedisCommands<String, String> redisCommand;
  protected Injector injector;

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
        bind(RedisConfig.class).toInstance(redisConfig);
        bind(IRedisConnectionPool.class).toInstance(redisPool);
        bind(ObjectMapper.class).toInstance(ObjectMapper);
      }
    });

  }

  @AfterEach
  public void afterEach() {
    if (redisCommand != null)
      Mockito.reset(redisCommand);
  }
}