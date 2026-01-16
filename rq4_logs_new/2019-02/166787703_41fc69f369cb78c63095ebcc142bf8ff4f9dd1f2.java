package com.example.mobile_client;

import android.Manifest;
import android.app.ProgressDialog;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.View;
import android.webkit.MimeTypeMap;
import android.widget.Button;
import android.widget.Toast;

import com.nbsp.materialfilepicker.MaterialFilePicker;
import com.nbsp.materialfilepicker.ui.FilePickerActivity;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;




public class Upload_Page extends AppCompatActivity {

    Button Pick;




    private static final String TAG = "Upload_Page";
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_upload);


        getSupportActionBar().setDisplayShowHomeEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        Pick = (Button)findViewById(R.id.pick);

        if(Build.VERSION.SDK_INT >= Build.VERSION_CODES.M)
        {
            //Log.v(TAG, "index=8");
            if(ActivityCompat.checkSelfPermission(this, Manifest.permission.READ_EXTERNAL_STORAGE)!= PackageManager.PERMISSION_GRANTED)
            {
              //  Log.v(TAG, "index=1");
                requestPermissions(new String[]{Manifest.permission.READ_EXTERNAL_STORAGE},100);
                return;
            }
        }

        enable_button();
    }


    private  void enable_button()
    {
        Pick.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new MaterialFilePicker()
                        .withActivity(Upload_Page.this)
                        .withRequestCode(10)
                        .start();
            }
        });
    }


    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grandResults)
    {
        //Log.v(TAG, "index=7");
        if(requestCode == 100 && (grandResults[0] == PackageManager.PERMISSION_GRANTED))
        {
            enable_button();
            //Log.v(TAG, "index=3");
        }
        else
        {
            if(Build.VERSION.SDK_INT >= Build.VERSION_CODES.M)
            {
               // Log.v(TAG, "index=0000000000");
                requestPermissions(new String[]{Manifest.permission.READ_EXTERNAL_STORAGE},100);
               // Log.v(TAG, "index=4");
            }
        }
    }


    ProgressDialog process;
    @Override
    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);

        //super.onActivityResult(requestCode, resultCode, data);
        if(requestCode == 10 && resultCode == RESULT_OK)
        {
            process = new ProgressDialog(Upload_Page.this);
            process.setTitle("Uploading");
            process.setMessage("Please waiting");
            process.show();

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    //select the file
                    File f = new File(data.getStringExtra(FilePickerActivity.RESULT_FILE_PATH));
                    // the type of file
                    String content_type =  getMimeType(f.getPath());


                    String file_path = f.getAbsolutePath();

                    OkHttpClient client = new OkHttpClient();

                    String username = (String) MySharedPreferences.getuserName(Upload_Page.this);

                    RequestBody file_body = RequestBody.create(MediaType.parse(content_type),f);


                    RequestBody requestBody = new MultipartBody.Builder()
                            .setType(MultipartBody.FORM)
                            .addFormDataPart("file", f.getName(),
                                    RequestBody.create(MediaType.parse("multipart/form-data"), f))
                            .addFormDataPart("username", username)
                            .addFormDataPart("url","")
                            .build();

                    Request request = new Request.Builder()
                            .url("http://teamparamount.cn:8080/Paramount/upload")
                            .post(requestBody)
                            .build();





                    try{
                        Response response = client.newCall(request).execute();
                        //Toast.makeText(Upload_Page.this, "works", Toast.LENGTH_SHORT).show();
                        if(!response.isSuccessful())
                        {
                            throw new IOException("Error" + response);
                        }
                        process.dismiss();
                    }catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
        }


    }
    //get the file's extension
    private String getMimeType(String path)
    {
        String extension = MimeTypeMap.getFileExtensionFromUrl(path);
        return MimeTypeMap.getSingleton().getMimeTypeFromExtension(extension);
    }


    @Override
    public boolean onOptionsItemSelected(MenuItem item)
    {
        switch (item.getItemId())
        {
            case android.R.id.home:
                finish();
                break;

            default:
                //no error
        }
        return super.onOptionsItemSelected(item);
    }
    @Override
    public boolean onCreateOptionsMenu(Menu menu)
    {
        MenuInflater inflater = getMenuInflater();
        inflater.inflate(R.menu.toobar_edit_menu,menu);
        return super.onCreateOptionsMenu(menu);
    }







}