package com.example.chores.ui.forms;

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
import com.example.chores.ativities.BoardActivity;
import com.example.chores.classes.Board;
import com.example.chores.classes.User;

import org.json.JSONException;
import org.json.JSONObject;

public class NewBoardFormActivity extends AppCompatActivity {

    private EditText title;
    private EditText description;
    private Button createBoardButton;
    private String url = "https://chores-backend-atqwerty.herokuapp.com/board/create";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_new_board_form);

        Intent intent = getIntent();
        final User currentUser = (User) intent.getSerializableExtra("currentUser");

        title = findViewById(R.id.new_board_title_id);
        description = findViewById(R.id.new_board_decription_id);
        createBoardButton = findViewById(R.id.new_board_create);

        createBoardButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                JSONObject newBoard = new JSONObject();
                try {
                    newBoard.put("title", title.getText());
                    newBoard.put("description", description.getText());
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                JsonObjectRequest req = new JsonObjectRequest(Request.Method.POST, url,
                        newBoard, new Response.Listener<JSONObject>() {
                    @Override
                    public void onResponse(JSONObject response) {
                        Board freshCreatedBoard = null;
                        try {
                            freshCreatedBoard = new Board(response.getInt("ID"),
                                    response.getString("title"),
                                    response.getString("description"),
                                    currentUser,
                                    getApplicationContext()
                                    );
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        Intent intent = new Intent(getApplicationContext(), BoardActivity.class);
                        intent.putExtra("targetBoard", freshCreatedBoard);
                        startActivity(intent);
                        finish();
                    }
                }, new Response.ErrorListener() {
                    @Override
                    public void onErrorResponse(VolleyError error) {
                        Log.d("adsf", "onErrorResponse: " + error.getMessage());

                    }
                });

                AppController.getInstance(getApplicationContext()).addToRequestQueue(req, "CreateNewBoard");
            }
        });
    }
}