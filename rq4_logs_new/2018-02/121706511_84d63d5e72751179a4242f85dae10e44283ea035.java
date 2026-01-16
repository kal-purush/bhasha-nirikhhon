package janjira.jiraporn.yonlada.aroirestuarant.utility;

import android.content.Context;
import android.os.AsyncTask;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

/**
 * Created by TOA on 2/16/2018.
 */


public class LoadAllJSON extends AsyncTask<String, Void, String>{

    private Context context;

    public LoadAllJSON(Context context) {
        this.context = context;
    }

    @Override
    protected String doInBackground(String... strings) {
      try {
          OkHttpClient okHttpClient = new OkHttpClient();
          Request.Builder builder = new Request.Builder();
          Request request = builder.url(strings[0]).build();
          Response response = okHttpClient.newCall(request).execute();
          return response.body().string();



      }catch (Exception e) {
          e.printStackTrace();
          return null;
      }


    }
}   // Main Class