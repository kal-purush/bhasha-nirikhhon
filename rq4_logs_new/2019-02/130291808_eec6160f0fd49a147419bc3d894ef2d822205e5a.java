package com.github.ssullivan.db.redis;

import com.github.ssullivan.RedisConfig;
import com.github.ssullivan.db.IFacilityDao;
import com.github.ssullivan.db.IndexFacility;
import com.github.ssullivan.guice.RedisClientModule;
import com.github.ssullivan.model.Facility;
import com.github.ssullivan.utils.ShortUuid;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.lettuce.core.RedisClient;
import java.util.List;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class FacilitExpireIntegrationTest {
  private static final String FeedId = "tvkT3WN7nBbnpPHredaHb2";
  @Test
  public void testExpire() throws Exception {
    final RedisConfig redisConfig = new RedisConfig("localhost", 6379, 5);
    redisConfig.setTimeout(250);


    final Injector injector = Guice
        .createInjector(new RedisClientModule(redisConfig));

    final RedisClient client = injector.getInstance(RedisClient.class);
    client.connect().sync().flushdb();



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
    final Boolean result = facilityDao.expire(FeedId, 1, false);

    final IndexFacility indexFacility = injector.getInstance(IndexFacility.class);
    indexFacility.expire(FeedId, 1, false);

    MatcherAssert.assertThat(result, Matchers.equalTo(true));

    int retries = 0;
    do {
      List<String> keys = client.connect().sync().keys("*");
      if (keys == null || keys.size() <= 0) {
        break;
      }
      Thread.sleep(150 + (retries * 5));
    } while (++retries < 10);


    List<String> keys = client.connect().sync().keys("*");
    MatcherAssert.assertThat(keys.size(), Matchers.lessThanOrEqualTo(0));

    final Facility fromRedisAfterExpire = facilityDao.getFacility(facility.getId());
    MatcherAssert.assertThat(fromRedisAfterExpire, Matchers.nullValue());
  }
}