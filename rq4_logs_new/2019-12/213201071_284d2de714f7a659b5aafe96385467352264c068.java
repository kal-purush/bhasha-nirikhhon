package com.example.chores.ui.forms;

import androidx.appcompat.app.AppCompatActivity;
import androidx.navigation.ui.AppBarConfiguration;

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
import com.example.chores.classes.Task;

import org.json.JSONException;
import org.json.JSONObject;

public class NewTaskFormActivity extends AppCompatActivity {

    private EditText title;
    private EditText description;
    private EditText status;
    private Button newTaskButton;
    private Board board;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_new_task_form);

        Intent intent = getIntent();
        board = (Board) intent.getSerializableExtra("board");

        title = findViewById(R.id.new_task_title_id);
        description = findViewById(R.id.new_task_decription_id);
        status = findViewById(R.id.new_task_status_id);
        newTaskButton = findViewById(R.id.new_task_create);

        newTaskButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String url = "https://chores-backend-atqwerty.herokuapp.com/board/" + board.getId() + "/task/create";
                Log.d("asdf", "onClick: " + url);
                JSONObject newTask = new JSONObject();
                try {
                    newTask.put("title", title.getText());
                    newTask.put("description", description.getText());
                    newTask.put("status", Integer.valueOf(String.valueOf(status.getText())));
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                JsonObjectRequest req = new JsonObjectRequest(Request.Method.POST, url,
                        newTask, new Response.Listener<JSONObject>() {
                    @Override
                    public void onResponse(JSONObject response) {
                        Task freshCreatedTask = null;
                        try {
                            freshCreatedTask = new Task(response.getInt("ID"),
                                    response.getString("title"),
                                    response.getString("status"),
                                    response.getString("description")
                            );
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        board.addTask(freshCreatedTask);
                        Intent intent = new Intent(getApplicationContext(), BoardActivity.class);
                        intent.putExtra("targetBoard", board);
                        startActivity(intent);
                        finish();
                    }
                }, new Response.ErrorListener() {
                    @Override
                    public void onErrorResponse(VolleyError error) {
                        Log.d("adsf", "onErrorResponse: " + error.getMessage());
                    }
                });

                AppController.getInstance(getApplicationContext()).addToRequestQueue(req, "CreateNewTask");
            }
        });
    }
}