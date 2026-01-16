package com.example.chores.ativities;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

import com.android.volley.Request;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.JsonObjectRequest;
import com.example.chores.AppController;
import com.example.chores.R;
import com.example.chores.classes.Board;
import com.example.chores.classes.Status;

import org.json.JSONException;
import org.json.JSONObject;

public class NewStatusActivity extends AppCompatActivity {

    private EditText statusTitle;
    private Button createStatusButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_new_status);

        final Board currentBoard = (Board) getIntent().getSerializableExtra("currentBoard");

        statusTitle = findViewById(R.id.new_status_title);
        createStatusButton = findViewById(R.id.new_status_create);

        createStatusButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String url = "https://chores-backend-atqwerty.herokuapp.com/board/newStatusMobile";

                JSONObject status = new JSONObject();
                try {
                    status.put("id", currentBoard.getId());
                    status.put("status", statusTitle.getText());
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                JsonObjectRequest req = new JsonObjectRequest(Request.Method.POST, url, status,
                        new Response.Listener<JSONObject>() {
                            @Override
                            public void onResponse(JSONObject response) {
                                try {
                                    currentBoard.addStatus(new Status(response.getInt("ID"),
                                            response.getString("status")));
                                    Intent intent = new Intent(getApplicationContext(), BoardActivity.class);
                                    intent.putExtra("targetBoard", currentBoard);
                                    startActivity(intent);
                                } catch (JSONException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, new Response.ErrorListener() {
                    @Override
                    public void onErrorResponse(VolleyError error) {
                        Log.d("adsf", "onErrorResponse: " + error.getMessage());
                    }
                });

                AppController.getInstance(getApplicationContext()).addToRequestQueue(req, "newStatus");
            }
        });
    }
}