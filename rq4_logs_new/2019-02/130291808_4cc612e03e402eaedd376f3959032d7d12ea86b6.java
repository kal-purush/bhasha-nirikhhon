package com.github.ssullivan.core;

import com.github.ssullivan.db.ICategoryCodesDao;
import com.github.ssullivan.model.AvailableServices;
import com.github.ssullivan.model.Category;
import com.github.ssullivan.model.Facility;
import com.github.ssullivan.model.Service;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.dropwizard.lifecycle.Managed;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AvailableServiceController implements IAvailableServiceController, Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvailableServiceController.class);
  private final ExecutorService pool = Executors.newSingleThreadExecutor();

  private final ICategoryCodesDao categoryCodesDao;
  private final LoadingCache<String, Category> cache;

  @Inject
  public AvailableServiceController(final ICategoryCodesDao categoryCodesDao) {
    this.categoryCodesDao = categoryCodesDao;
    this.cache = CacheBuilder.newBuilder()
        .concurrencyLevel(8)
        .maximumSize(256)
        .refreshAfterWrite(30, TimeUnit.MINUTES)
        .expireAfterAccess(1, TimeUnit.DAYS)
        .build(new CategoryCacheLoader(pool, categoryCodesDao));
  }

  @Override
  public List<Facility> applyList(final List<Facility> facilities) {
    facilities.forEach(this::apply);
    return facilities;
  }

  @Override
  public Facility apply(final Facility facility) {
    final ImmutableMap<String, Category> cats = this.cache.getAllPresent(facility.getCategoryCodes());


    // For the available services object we only want the category objects included in it
    // to contains services that the facility actually has
    final Set<Category> facilityCats = cats.values().stream().map(category -> {
      // This contains only the service objects that the facility actually has
      final Set<Service> facilityServices =
          category.getServices().stream().filter(service -> facility.hasAnyOf(service.getCode())).collect(
          Collectors.toSet());

      // Create a new category that only has the services the facility has
      return new Category(category.getCode(), category.getName(), facilityServices);
    }).collect(Collectors.toSet());


    facility.setAvailableServices(new AvailableServices(facilityCats));

    return facility;
  }

  public void refreshAll() {
    try {
      LOGGER.info("Refreshing category list from db");
      final List<Category> categoryList = this.categoryCodesDao.listCategories();
      categoryList.forEach(cat -> this.cache.put(cat.getCode(), cat));
      LOGGER.info("Loaded {} cats", categoryList.size());
    } catch (IOException e) {
      LOGGER.error("Failed to list categories!", e);
    }
  }

  @Override
  public void start() throws Exception {
    refreshAll();
  }

  @Override
  public void stop() throws Exception {
    this.pool.shutdown();
  }

  /**
   * CacheLoader that can asycn refresh the cache
   */
  private static final class CategoryCacheLoader extends CacheLoader<String, Category> {
    private final ListeningExecutorService executorService;
    private final ICategoryCodesDao catsDao;

    private CategoryCacheLoader(final ExecutorService threadPool, final ICategoryCodesDao catsDao) {
      this.executorService = MoreExecutors.listeningDecorator(threadPool);
      this.catsDao = catsDao;
    }

    @Override
    public Category load(final String key) throws Exception {
      Objects.requireNonNull(key, "Category must not be null");
      return this.catsDao.get(key);
    }

    @Override
    public ListenableFuture<Category> reload(final String key, final Category oldValue) throws Exception {
      return this.executorService.submit(() -> this.catsDao.get(key));
    }
  }
}