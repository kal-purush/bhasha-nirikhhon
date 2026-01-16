package hm.orz.chaos114.android.circlewatcher.di;

import android.content.Context;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;
import hm.orz.chaos114.android.circlewatcher.App;
import hm.orz.chaos114.android.circlewatcher.network.CircleCiService;
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

@Module
public class AppModule {
    private static final String CIRCLE_CI_URL = "https://circleci.com/api/v1.1/";

    private final App application;

    public AppModule(App application) {
        this.application = application;
    }

    @Provides
    @Singleton
    Context provideApplicationContext() {
        return application.getApplicationContext();
    }

    @Provides
    @Singleton
    public OkHttpClient provideHttpClient(Interceptor interceptor) {
        HttpLoggingInterceptor logging = new HttpLoggingInterceptor(message -> Timber.tag("OkHttp").d(message));
        logging.setLevel(HttpLoggingInterceptor.Level.BODY);
        return new OkHttpClient.Builder()
                .addInterceptor(logging)
                .addInterceptor(interceptor)
                .build();
    }

    @Provides
    public Interceptor provideInterceptor(Context context) {
        return chain -> {
            Request request = chain.request();
            String apiToken = SharedPreferenceUtil.getApiToken(context);
            HttpUrl url = request.url().newBuilder().addQueryParameter("circle-token", apiToken).build();
            Request newRequest = request.newBuilder()
                    .url(url)
                    .addHeader("Accept", "application/json")
                    .build();

            return chain.proceed(newRequest);
        };
    }

    @Provides
    public Gson provideGson() {
        return new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create();
    }

    @Provides
    @Singleton
    public CircleCiService provideCircleCiService(OkHttpClient client, Gson gson) {
        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(CIRCLE_CI_URL)
                .client(client)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .addCallAdapterFactory(RxJavaCallAdapterFactory.create())
                .build();
        return retrofit.create(CircleCiService.class);

    }
}