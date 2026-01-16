package com.numan1617.tfltubedepartures.network.retrofit;

import android.support.annotation.NonNull;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.numan1617.tfltubedepartures.network.TflService;
import com.numan1617.tfltubedepartures.network.model.StopPoint;
import com.numan1617.tfltubedepartures.network.model.StopPointResponse;
import com.numan1617.tfltubedepartures.network.model.StopProperty;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.logging.HttpLoggingInterceptor;
import java.util.List;
import retrofit.RestAdapter;
import retrofit.client.OkClient;
import retrofit.converter.GsonConverter;
import retrofit.http.GET;
import rx.Observable;

public class RetrofitTflService implements TflService {

  private final Api api;
  private final String appId;
  private final String apiKey;

  public RetrofitTflService(@NonNull final String appId, @NonNull final String apiKey) {
    this.appId = appId;
    this.apiKey = apiKey;

    final HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
    logging.setLevel(HttpLoggingInterceptor.Level.HEADERS);

    final OkHttpClient okHttpClient = new OkHttpClient();
    okHttpClient.interceptors().add(logging);
    okHttpClient.interceptors().add(chain -> {
      final Request request = chain.request();
      final HttpUrl url = request.httpUrl()
          .newBuilder()
          .addQueryParameter("app_id", appId)
          .addQueryParameter("app_key", apiKey)
          .build();
      final Request modifiedRequest = request.newBuilder().url(url).build();
      return chain.proceed(modifiedRequest);
    });

    final Gson gson =
        new GsonBuilder().registerTypeAdapterFactory(StopPointResponse.typeAdapterFactory())
            .registerTypeAdapterFactory(StopPoint.typeAdapterFactory())
            .registerTypeAdapterFactory(StopProperty.typeAdapterFactory())
            .create();

    final RestAdapter restAdapter = new RestAdapter.Builder().setEndpoint("https://api.tfl.gov.uk")
        .setConverter(new GsonConverter(gson))
        .setClient(new OkClient(okHttpClient))
        .build();

    this.api = restAdapter.create(Api.class);
  }

  @Override public Observable<List<StopPoint>> stopPoint() {
    return api.stopPoint().map(StopPointResponse::stopPoints);
  }

  private interface Api {
    @GET("/StopPoint?lat=51.5065&lon=-0.2081&stopTypes=NaptanMetroStation,NaptanRailStation&radius=900&useStopPointHierarchy=false&returnLines=True")
    Observable<StopPointResponse> stopPoint();
  }
}