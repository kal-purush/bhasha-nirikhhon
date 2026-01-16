package com.example.gamjaproject_now;

import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;
import android.os.Bundle;
import android.util.Log;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;

import com.example.gamjaproject_now.API.Content;
import com.example.gamjaproject_now.API.APIController;
import com.example.gamjaproject_now.API.ContentGenre;
import com.example.gamjaproject_now.API.Count;
import com.example.gamjaproject_now.API.Genre;


import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class RandomActivity extends AppCompatActivity {


    ImageView[] programV;
    TextView[] programNA;

    TextView[] programDi;

    TextView[] programSU;
    String[] tableList = {"couplay", "kakaowebtoon", "kpnovel", "naverwebtoon", "netflix", "watcha"};
    String[] GenretableList = {"couplay_genre", "kakaowebtoon_genre", "kpnovel_genre", "naverwebtoon_genre", "netflix_genre", "watcha_genre"};

    private Content[] resultG;

    private Content[] result;

    String genretableN;
    int Gid;
    int GC;
    Bitmap bitmap;
    int index = 0;
    int All;
    Random rand;
    int PageRandom;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        EdgeToEdge.enable(this);
        setContentView(R.layout.activity_random);



        programV[0] = (ImageView) findViewById(R.id.randomprogramView);
        programNA[0] = (TextView) findViewById(R.id.randomprogramname);
        programSU[0] = (TextView) findViewById(R.id.randomprogramsummary);
        programDi[0] = (TextView) findViewById(R.id.randomprogramdirector);

        fetchDataFromApi();




//        137, 224, 4816, 657, 305, 200;



    }

    private void fetchDataFromApi() {
            String tableName = tableList[rand.nextInt(tableList.length)];
            if (tableName.equals(tableList[0])) {
                PageRandom = (int) (Math.random() * 137) + 1;
            } else if (tableName.equals(tableList[1])) {
                PageRandom = (int) (Math.random() * 224) + 1;
            } else if (tableName.equals(tableList[2])) {
                PageRandom = (int) (Math.random() * 4816) + 1;
            } else if (tableName.equals(tableList[3])) {
                PageRandom = (int) (Math.random() * 657) + 1;
            } else if (tableName.equals(tableList[4])) {
                PageRandom = (int) (Math.random() * 305) + 1;
            } else if (tableName.equals(tableList[5])) {
                PageRandom = (int) (Math.random() * 200) + 1;
            }
            Call<Content[]> call = APIController.getTestCall(tableName, PageRandom, 1);
            call.enqueue(new Callback<Content[]>() {
                @Override
                public void onResponse(Call<Content[]> call, Response<Content[]> response) {
                    if (response.isSuccessful()) {
                        result = response.body();
                        if(result != null) {
                            programNA[0].setText(result[0].getTitle());
                            programDi[0].setText(result[0].getDirector());
                            programSU[0].setText(result[0].getDescription());
                            new RandomActivity.DownloadFilesTask().execute(result[0].getImg());
                        }


                    } else {
                        Toast.makeText(RandomActivity.this, "API call failed", Toast.LENGTH_SHORT).show();
                    }
                }

                @Override
                public void onFailure(Call<Content[]> call, Throwable t) {
                    Log.d("결과", "실패 : " + t.getMessage());
                }
            });

        }




    private class DownloadFilesTask extends AsyncTask<String, Void, Bitmap> {
        @Override
        protected Bitmap doInBackground(String... strings) {
            Bitmap bmp = null;
            try {
                String img_url = strings[0];
                URL url = new URL(img_url);
                bmp = BitmapFactory.decodeStream(url.openConnection().getInputStream());
            } catch (MalformedURLException e) {
                Log.e("DownloadFilesTask", "MalformedURLException: " + e.getMessage());
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("DownloadFilesTask", "IOException: " + e.getMessage());
                e.printStackTrace();
            }
            return bmp;
        }

        @Override
        protected void onPostExecute(Bitmap result) {
            if (resultG != null) {
                programV[0].setImageBitmap(result);
            } else {
                Log.e("DownloadFilesTask", "Bitmap is null");
            }
        }
    }

}

