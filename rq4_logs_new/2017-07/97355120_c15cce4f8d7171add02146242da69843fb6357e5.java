package com.zimincom.battlegroundstats;

import com.zimincom.battlegroundstats.StatObjects.UserInfo;

import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Path;

/**
 * Created by Zimincom on 2017. 7. 21..
 */

public interface RemoteService {
    String BASE_URL = "http://pubgtracker.com/" ;

    @Headers("TRN-Api-Key: cab8aadf-c4c5-4652-97ab-79bb53b38014")
    @GET("api/profile/pc/{pubg-nickname}")
    Call<UserInfo> getUserInfo(@Path("pubg-nickname") String playerNickname);

    Retrofit retrofit = new Retrofit.Builder()
            .baseUrl(BASE_URL)
            .build();
}