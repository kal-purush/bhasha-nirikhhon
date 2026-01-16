package co.johnnyli.gamer;

import android.content.Intent;
import android.graphics.Color;
import android.graphics.drawable.ColorDrawable;
import android.os.Bundle;
import android.support.v7.app.ActionBar;
import android.support.v7.app.ActionBarActivity;
import android.util.Log;
import android.view.View;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;

import com.loopj.android.http.AsyncHttpClient;
import com.loopj.android.http.JsonHttpResponseHandler;
import com.loopj.android.http.RequestParams;

import org.json.JSONObject;

public class AddPost extends ActionBarActivity implements View.OnClickListener {
    private EditText post_text;
    private static final String postURL =
            "http://ec2-52-11-124-82.us-west-2.compute.amazonaws.com/api/posts";
    private String pk;
    private String name;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_post);
        setTitle("Add Post");
        //ActionBar Color
        String color = MainActivity.color;
        ActionBar bar = getSupportActionBar();
        bar.setBackgroundDrawable(new ColorDrawable(Color.parseColor(color)));
        getWindow().setSoftInputMode(WindowManager.LayoutParams.SOFT_INPUT_STATE_HIDDEN);
        pk = this.getIntent().getExtras().getString("group");
        name = this.getIntent().getExtras().getString("name");
        post_text = (EditText) findViewById(R.id.post_text);
        Button post_button = (Button) findViewById(R.id.post_button);
        post_button.setOnClickListener(this);
        Button cancel_button = (Button) findViewById(R.id.cancel_button);
        cancel_button.setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        final Intent group = new Intent(this, Group.class);
        group.putExtra("pk", pk);
        group.putExtra("name", name);

        if(v.getId() == R.id.post_button) {
            RequestParams params = new RequestParams();
            params.put("text", post_text.getText().toString());
            params.put("group", pk);
            params.put("owner", "4");
            AsyncHttpClient client = new AsyncHttpClient();
            client.addHeader("X-CSRFToken", Info.csrftoken);
            client.addHeader("Authorization", MainActivity.auth);

            client.post(postURL, params, new JsonHttpResponseHandler() {
                @Override
                public void onSuccess(JSONObject jsonObject) {
                    finish();
                    startActivity(group);
                }

                @Override
                public void onFailure(int statusCode, Throwable throwable, JSONObject error) {
                    Log.d("Error", error.toString());
                }
            });
        } else if (v.getId() == R.id.cancel_button) {
            finish();
            startActivity(group);
        }
        post_text.setText("");

    }
}