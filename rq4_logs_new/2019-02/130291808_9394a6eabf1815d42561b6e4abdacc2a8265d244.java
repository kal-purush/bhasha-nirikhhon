package com.github.ssullivan.tasks.feeds;

import com.github.ssullivan.RedisConfig;
import com.github.ssullivan.db.ICategoryCodesDao;
import com.github.ssullivan.db.IFacilityDao;
import com.github.ssullivan.db.IFeedDao;
import com.github.ssullivan.db.IServiceCodesDao;
import com.github.ssullivan.db.IndexFacility;
import com.github.ssullivan.db.redis.RedisCategoryCodesDao;
import com.github.ssullivan.db.redis.RedisServiceCodeDao;
import com.github.ssullivan.guice.RedisClientModule;
import com.github.ssullivan.model.Category;
import com.github.ssullivan.model.Facility;
import com.github.ssullivan.model.Service;
import com.github.ssullivan.utils.ShortUuid;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.lettuce.core.RedisClient;
import java.util.List;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class ManageFeedsTest {
  private static final String FeedId = "tvkT3WN7nBbnpPHredaHb2";

  @Test
  public void testExpireKeys() throws Exception {
    final RedisConfig redisConfig = new RedisConfig("localhost", 6379, 8);
    redisConfig.setTimeout(250);


    final Injector injector = Guice
        .createInjector(new RedisClientModule(redisConfig));

    final RedisClient client = injector.getInstance(RedisClient.class);
    client.connect().sync().flushdb();

    final ICategoryCodesDao categoryCodesDao = injector.getInstance(ICategoryCodesDao.class);
    final Category category = new Category();
    category.setCode("TEST");

    categoryCodesDao.addCategory(category);

    final IServiceCodesDao serviceCodesDao = injector.getInstance(IServiceCodesDao.class);
    final Service service = new Service();
    service.setCategoryCode("TEST");
    service.setCode("TEST");
    serviceCodesDao.addService(service);




    final IFacilityDao facilityDao = injector.getInstance(IFacilityDao.class);

    final Facility facility = new Facility();
    facility.setFeedId(FeedId);
    facility.setId(ShortUuid.randomShortUuid());
    facility.setName1("TEST");
    facility.setName2("TEST");
    facility.setCategoryCodes(ImmutableSet.of("TEST"));
    facility.setServiceCodes(ImmutableSet.of("TEST"));

    facilityDao.addFacility(FeedId, facility);

    final Facility fromRedis = facilityDao.getFacility(facility.getId());
    MatcherAssert.assertThat(fromRedis.getId(), Matchers.equalTo(facility.getId()));
    MatcherAssert.assertThat(fromRedis.getName1(), Matchers.equalTo(facility.getName1()));
    MatcherAssert.assertThat(fromRedis.getName2(), Matchers.equalTo(facility.getName2()));

    final IFeedDao feedDao = injector.getInstance(IFeedDao.class);
    feedDao.setCurrentFeedId(FeedId);
    feedDao.setSearchFeedId(FeedId);

    ManageFeeds manageFeeds = injector.getInstance(ManageFeeds.class);

    manageFeeds.expireOldFeeds(ShortUuid.randomShortUuid(), 1L);


    int retries = 0;
    do {
      List<String> keys = client.connect().sync().keys("*");
      if (keys == null || keys.size() <= 5) {
        break;
      }
      Thread.sleep(300 + (retries * 5));
    } while (++retries < 10);


    List<String> keys = client.connect().sync().keys("*");
    MatcherAssert.assertThat(keys.size(), Matchers.lessThanOrEqualTo(5));
    MatcherAssert.assertThat(client.connect().sync().ttl(RedisCategoryCodesDao.KEY), Matchers.equalTo(-1L));
    MatcherAssert.assertThat(client.connect().sync().ttl(RedisServiceCodeDao.KEY), Matchers.equalTo(-1L));

    final Facility fromRedisAfterExpire = facilityDao.getFacility(facility.getId());
    MatcherAssert.assertThat(fromRedisAfterExpire, Matchers.nullValue());
  }
}