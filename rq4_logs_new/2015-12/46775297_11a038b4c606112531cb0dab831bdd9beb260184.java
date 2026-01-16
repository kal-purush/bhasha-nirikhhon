package com.group33.greenthumb;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.support.v4.view.GestureDetectorCompat;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.ListView;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Daniel on 12/1/2015.
 */
public class TaskListActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_task_list);

        Button addButton = (Button) findViewById(R.id.plus_button);
        addButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent myIntent = new Intent(TaskListActivity.this, AddTaskActivity.class);
                TaskListActivity.this.startActivity(myIntent);
            }
        });

        Button deleteButton = (Button) findViewById(R.id.minus_button);
        deleteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent myIntent = new Intent(TaskListActivity.this, DeleteTaskActivity.class);
                TaskListActivity.this.startActivity(myIntent);
            }
        });

        Button editButton = (Button) findViewById(R.id.edit_button);
        editButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent myIntent = new Intent(TaskListActivity.this, EditTaskActivity.class);
                TaskListActivity.this.startActivity(myIntent);
            }
        });

        /*
        ListView taskListView = (ListView) findViewById(R.id.taskListView);

        String[] tasks = new String[] {"Water Roses", "Water Peonies", "Water Daisies", "Harvest Strawberries"};
        ArrayList<String> taskList = new ArrayList<String>();
        taskList.addAll(Arrays.asList(tasks));
        ArrayAdapter<String> listAdapter = new ArrayAdapter<String>(this, android.R.layout.simple_list_item_1, taskList);

        //listAdapter.add("task");
        taskListView.setAdapter( listAdapter );

        taskListView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                Intent intent = new Intent(TaskListActivity.this, EditTaskActivity.class);
                intent.putExtra("task", tasks[position]);
                startActivity(intent);
            }
        });
        */
    }
}