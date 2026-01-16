package urop.wifitracker;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * Created by tavish on 4/17/17.
 */

public class ApiClient {

    /*private static final String baseUrl= "10.20.141.133";*/
    private static final String baseUrl= "10.89.22.206";
    private static final int port=8000;

    public static Call getCoordinates()
    {
        HttpUrl httpUrl= new HttpUrl.Builder()
                .scheme("http")
                .host(baseUrl)
                .port(port)
                .addPathSegment("coordinates")
                .build();
        Request request= new Request.Builder()
                .get()
                .url(httpUrl)
                .build();
        return OkHttpSingleton.getOkHttpInstance().getOkHttpClient().newCall(request);
    }
}