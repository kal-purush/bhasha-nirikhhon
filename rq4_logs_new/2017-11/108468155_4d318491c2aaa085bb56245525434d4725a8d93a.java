package aaron.filecommand.activity;

import android.app.Activity;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.os.Handler;
import android.support.v7.app.AppCompatActivity;
import android.widget.ImageView;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

import aaron.filecommand.R;
import aaron.filecommand.base.BaseActivity;


/**
 * Created by jili on 8/17/17.
 */

public class SplashActivity extends BaseActivity {

    private ImageView imgSplash;
    /** Duration of wait **/
    private final int SPLASH_DISPLAY_LENGTH = 2000;
    private SharedPreferences sharedPrefs;
    @Override
    protected void initView(Bundle savedInstanceState) {
        setContentView(R.layout.activity_splash);
        imgSplash = (ImageView)findViewById(R.id.img_splash);
        sharedPrefs = getSharedPreferences("myPrefs", MODE_PRIVATE);

        new Handler().postDelayed(new Runnable(){
            @Override
            public void run() {
                /* Create an Intent that will start the Menu-Activity. */
                Intent mainIntent = new Intent(SplashActivity.this,MainActivity.class);
                SplashActivity.this.startActivity(mainIntent);
                SplashActivity.this.finish();
            }
        }, SPLASH_DISPLAY_LENGTH);
    }

    @Override
    protected void initData() {

    }

    @Override
    protected void loadData() {

    }
}