package com.marketplace.client;

import com.marketplace.client.model.ClassifiedAdRequest;
import com.marketplace.client.model.ClassifiedAdResponse;
import java.util.UUID;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;

public interface ClassifiedAdRestService {

  @POST("classified_ad")
  Call<Void> createClassifiedAd(@Body ClassifiedAdRequest classifiedAdRequest);

  @PUT("classified_ad/{classifiedAdId}/")
  Call<Void> updateClassifiedAd(@Path("classifiedAdId") @Body ClassifiedAdRequest classifiedAdRequest);

  @PUT("classified_ad/{classifiedAdId}/title")
  Call<Void> updateClassifiedAdTitle(@Path("classifiedAdId") UUID classifiedAdId, @Body ClassifiedAdRequest classifiedAdRequest);

  @PUT("classified_ad/{classifiedAdId}/text")
  Call<Void> updateClassifiedAdText(@Path("classifiedAdId") UUID classifiedAdId, @Body ClassifiedAdRequest classifiedAdRequest);

  @PUT("classified_ad/{classifiedAdId}/price")
  Call<Void> updateClassifiedAdPrice(@Path("classifiedAdId") UUID classifiedAdId, @Body ClassifiedAdRequest classifiedAdRequest);

  @PUT("classified_ad/{classifiedAdId}/approve")
  Call<Void> approveClassifiedAd(@Path("classifiedAdId") UUID classifiedAdId, @Body ClassifiedAdRequest classifiedAdRequest);

  @PUT("classified_ad/{classifiedAdId}/publish")
  Call<Void> publishClassifiedAd(@Path("classifiedAdId") UUID classifiedAdId, @Body ClassifiedAdRequest classifiedAdRequest);

  @GET("classified_ad/{classifiedAdId}")
  Call<ClassifiedAdResponse> getById(@Path("classifiedAdId") UUID classifiedAdId);

}