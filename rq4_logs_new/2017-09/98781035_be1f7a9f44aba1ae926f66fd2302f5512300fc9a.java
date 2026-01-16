package jp.techacademy.ryoichi.gokan.loginsample;

import android.content.SharedPreferences;
import android.preference.PreferenceManager;

import java.util.List;

import retrofit2.Call;
import retrofit2.http.Field;
import retrofit2.http.GET;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.POST;

/**
 * Created by houxianliangyi on 2017/09/02.
 */

public interface UserList {

    @GET("/v1/users")
    Call<List<User>> userList(@Header("Authorization") String token);
}