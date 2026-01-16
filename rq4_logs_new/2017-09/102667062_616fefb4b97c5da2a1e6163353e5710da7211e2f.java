package com.foo.umbrella.data;

import android.app.Application;
import com.jakewharton.picasso.OkHttp3Downloader;
import com.foo.umbrella.data.api.WeatherService;
import com.squareup.moshi.Moshi;
import com.squareup.picasso.Picasso;
import java.io.File;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava.RxJavaCallAdapterFactory;
import retrofit2.converter.moshi.MoshiConverterFactory;
import timber.log.Timber;

import static com.jakewharton.byteunits.DecimalByteUnit.MEGABYTES;

/**
 * Provides {@link Picasso} and {@link WeatherService}
 * that are all ready setup and ready to use.
 */
public final class ApiServicesProvider {

  private static final int DISK_CACHE_SIZE = (int) MEGABYTES.toBytes(50);

  private final WeatherService weatherService;
  private final Picasso picasso;

  /**
   * Constructor.
   *
   * @param application application context used for creating network caches.
   */
  public ApiServicesProvider(Application application) {

    OkHttpClient client = createOkHttpClient(application);
    Moshi moshi = provideMoshi();

    weatherService = provideRetrofit(client, moshi).create(WeatherService.class);

    picasso = new Picasso.Builder(application)
        .downloader(new OkHttp3Downloader(client))
        .listener((picasso, uri, e) -> Timber.e(e, "Failed to load image: %s", uri))
        .build();
  }

  public Picasso getPicasso() {
    return picasso;
  }

  /** @return an instance of the {@link WeatherService} service that is ready to use. */
  public WeatherService getWeatherService() {
    return weatherService;
  }

  private Retrofit provideRetrofit(OkHttpClient client, Moshi moshi) {
    return new Retrofit.Builder()
        .client(client)
        .baseUrl("http://api.wunderground.com")
        .addConverterFactory(MoshiConverterFactory.create(moshi))
        .addCallAdapterFactory(RxJavaCallAdapterFactory.create())
        .build();
  }

  private OkHttpClient createOkHttpClient(Application app) {
    // Install an HTTP cache in the application cache directory.
    File cacheDir = new File(app.getCacheDir(), "http");
    Cache cache = new Cache(cacheDir, DISK_CACHE_SIZE);

    return new OkHttpClient.Builder()
        .cache(cache)
        .build();
  }

  private Moshi provideMoshi() {
    return new Moshi.Builder()
        .add(new ForecastConditionAdapter())
        .add(MoshiAdapterFactory.create())
        .build();
  }
}