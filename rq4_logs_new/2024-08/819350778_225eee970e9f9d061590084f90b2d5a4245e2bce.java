package com.app.Regional_News;

import static com.app.Regional_News.extra.UtilsApi.BASE_URL_API;

import android.os.Bundle;
import android.view.KeyEvent;
import android.view.MenuItem;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.app.Regional_News.data.Udata;
import com.app.Regional_News.extra.BaseApiService;
import com.app.Regional_News.extra.SharedPrefManager;
import com.app.Regional_News.extra.UtilsApi;
import com.google.gson.Gson;
import com.squareup.picasso.Picasso;

public class ProfileUpdationActivation extends AppCompatActivity {

    TextView current_name,current_email;
    BaseApiService mApiService;
    Udata fp;
    public  static  String uid;
    SharedPrefManager sharedPrefManager;
    ImageView imageView;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile_updation_activation);

        mApiService = UtilsApi.getAPIService();


        // USE FOR DISPLAY SYSTEM INBUILT BACK NAVIGATION ARROW
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        // Set the toolbar title
        getSupportActionBar().setTitle("Profile Updation");


        //DATA FETCHING FROM USER
        sharedPrefManager = new SharedPrefManager(this);
        String fdata = sharedPrefManager.getFdata();
        Gson gson = new Gson();
        fp = gson.fromJson(fdata, Udata.class);


        current_name = findViewById(R.id.current_name);
        current_email = findViewById(R.id.current_email);
        imageView = findViewById(R.id.imageView);


        current_name.setText(fp.getU_name());
        current_email.setText(fp.getU_email());

        String c_pic = BASE_URL_API + fp.getU_pic();
        uid = fp.getU_id();
        Picasso.get()
                .load(c_pic)
                .resize(100, 100)
                .centerCrop()
                .into(imageView);


    }

    // IMPORTANT FOR BACK NAVIGATION BY ARROW
    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if (keyCode == KeyEvent.KEYCODE_BACK) {
            finish();
            return true;
        }
        return super.onKeyDown(keyCode, event);
    }

    // IMPORTANT FOR BACK NAVIGATION BY ARROW
    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            finish();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }
}