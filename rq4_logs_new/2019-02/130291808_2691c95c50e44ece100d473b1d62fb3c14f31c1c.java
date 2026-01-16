package com.github.ssullivan.db.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ssullivan.RedisConfig;
import com.github.ssullivan.db.ICategoryCodesDao;
import com.github.ssullivan.guice.RedisClientModule;
import com.github.ssullivan.model.Category;
import com.github.ssullivan.model.Service;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.RedisClient;
import java.io.IOException;
import java.util.List;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.Test;

public class CategoryDaoIntegrationTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testJackson() throws IOException {
    Service service = new Service();
    service.setName("TEST");
    service.setCode("TEST");
    service.setDescription("TEST");
    service.setCategoryCode("TEST");

    Category category = new Category();
    category.setName("TEST");
    category.setCode("TEST");
    category.setServiceCodes(Sets.newHashSet("TEST"));
    category.setServices(Sets.newHashSet(service));

    final String categoryJson = OBJECT_MAPPER.writeValueAsString(category);

    final Category fromJson = OBJECT_MAPPER.readValue(categoryJson, Category.class);
    MatcherAssert.assertThat(fromJson.getName(), Matchers.equalTo(category.getName()));
    MatcherAssert.assertThat(fromJson.getCode(), Matchers.equalTo(category.getCode()));
    MatcherAssert.assertThat(fromJson.getServiceCodes(), Matchers.containsInAnyOrder("TEST"));
  }

  @Test
  public void testAddingToRedis() throws IOException {
    final RedisConfig redisConfig = new RedisConfig("localhost", 6379, 10);
    redisConfig.setTimeout(250);

    final ICategoryCodesDao categoryCodesDao = Guice.createInjector(new RedisClientModule(redisConfig)).getInstance(
        ICategoryCodesDao.class);

    Service service = new Service();
    service.setName("TEST");
    service.setCode("TEST");
    service.setDescription("TEST");
    service.setCategoryCode("TEST");

    Category category = new Category();
    category.setName("TEST");
    category.setCode("TEST");
    category.setServiceCodes(Sets.newHashSet("TEST"));
    category.setServices(Sets.newHashSet(service));

    MatcherAssert.assertThat(categoryCodesDao.addCategory(category), Matchers.equalTo(true));


    final List<Category> fromRedis = categoryCodesDao.listCategories();
    MatcherAssert.assertThat(fromRedis, Matchers.notNullValue());
    MatcherAssert.assertThat(fromRedis.size(), Matchers.equalTo(1));

    final Category cat = fromRedis.get(0);
    MatcherAssert.assertThat(cat.getName(), Matchers.equalTo(category.getName()));
    MatcherAssert.assertThat(cat.getCode(), Matchers.equalTo(category.getCode()));
    MatcherAssert.assertThat(cat.getServiceCodes(), Matchers.containsInAnyOrder("TEST"));

    MatcherAssert.assertThat(categoryCodesDao.delete(category.getCode()), Matchers.equalTo(true));

  }
}