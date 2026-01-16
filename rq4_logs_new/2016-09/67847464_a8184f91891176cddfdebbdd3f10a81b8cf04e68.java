package hm.orz.chaos114.android.circlewatcher.network;

import android.content.Context;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import hm.orz.chaos114.android.circlewatcher.util.SharedPreferenceUtil;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava.RxJavaCallAdapterFactory;
import retrofit2.converter.gson.GsonConverterFactory;
import timber.log.Timber;

/**
 * Manage Retrofit Client.
 */
public final class ApiClient {
    private static final String CIRCLE_CI_URL = "https://circleci.com/api/v1.1/";

    private ApiClient() {
        // prevent instantiate
    }

    public static CircleCiService getClient(final Context context) {
        Interceptor authInterceptor = chain -> {
            Request request = chain.request();
            String apiToken = SharedPreferenceUtil.getApiToken(context);
            HttpUrl url = request.url().newBuilder().addQueryParameter("circle-token", apiToken).build();
            Request newRequest = request.newBuilder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .build();

            return chain.proceed(newRequest);
        };
        HttpLoggingInterceptor logging = new HttpLoggingInterceptor(message -> Timber.tag("OkHttp").d(message));
        logging.setLevel(HttpLoggingInterceptor.Level.BODY);
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(logging)
                .addInterceptor(authInterceptor)
                .build();

        Gson gson = new GsonBuilder()
                .setDateFormat("yyyy-MM-dd HH:mm:ss")
                .create();
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(CIRCLE_CI_URL)
                .client(client)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .addCallAdapterFactory(RxJavaCallAdapterFactory.create())
                .build();
        return retrofit.create(CircleCiService.class);
    }
}