package com.app.Regional_News;

import android.os.Build;
import android.os.Bundle;
import android.view.KeyEvent;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.app.Regional_News.data.Feedbackdata;
import com.app.Regional_News.data.Udata;
import com.app.Regional_News.extra.BaseApiService;
import com.app.Regional_News.extra.SharedPrefManager;
import com.app.Regional_News.extra.UtilsApi;
import com.google.gson.Gson;

import org.json.JSONException;
import org.json.JSONObject;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class FeedbackActivity extends AppCompatActivity {

    Button bt_submit;
    EditText feedback_title,feedback_desc;
    BaseApiService mApiService;
    Udata fp;
    SharedPrefManager sharedPrefManager;

    @RequiresApi(api = Build.VERSION_CODES.M)

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_feedback);

        mApiService = UtilsApi.getAPIService();

        // USE FOR DISPLAY SYSTEM INBUILT BACK NAVIGATION ARROW
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        // Set the toolbar title
        getSupportActionBar().setTitle("Feedback");

        //DATA FETCHING FROM USER
        sharedPrefManager = new SharedPrefManager(this);
        String fdata = sharedPrefManager.getFdata();
        Gson gson = new Gson();
        fp = gson.fromJson(fdata, Udata.class);


        feedback_title=findViewById(R.id.feedback_title);
        feedback_desc=findViewById(R.id.feedback_desc);
        bt_submit=findViewById(R.id.bt_submit);



        bt_submit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String f_title= feedback_title.getText().toString();
                String f_desc = feedback_desc.getText().toString();

                if(!f_title.isEmpty()&&!f_desc.isEmpty())
                {
                    JSONObject j1=new JSONObject();
                    try {
                        j1.put("sm_u_id",fp.getU_id());
                        j1.put("f_title",f_title);
                        j1.put("f_desc",f_desc);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }

                    String data=j1.toString();
                    addfeedback(data);

                }
                else
                {
                    Toast.makeText(FeedbackActivity.this,"Please Enter Feedback Title and Description",Toast.LENGTH_SHORT).show();
                }
//
            }
        });

    }

    private void addfeedback(String data) {

        mApiService.rnsFeedbackRequest("add_feedback",data).enqueue(new Callback<Feedbackdata>() {
            @Override
            public void onResponse(Call<Feedbackdata> call, Response<Feedbackdata> response) {
                if (response.isSuccessful()){
                    Feedbackdata udata=response.body();
                    if (udata.getStatus().equals("1")){
                        String msg=udata.getMsg();
                        Toast.makeText(FeedbackActivity.this,""+msg,Toast.LENGTH_SHORT).show();
                        feedback_title.setText("");
                        feedback_desc.setText("");
                    }
                    else
                    {
                        String msg=udata.getMsg();
                        Toast.makeText(FeedbackActivity.this,""+msg,Toast.LENGTH_SHORT).show();
                    }
                }



            }

            @Override
            public void onFailure(Call<Feedbackdata> call, Throwable t) {

            }
        });
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