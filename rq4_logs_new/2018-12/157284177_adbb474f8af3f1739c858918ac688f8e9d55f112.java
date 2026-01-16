package com.example.gav.taskmanager.features.newtask;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.FrameLayout;

import com.example.gav.taskmanager.R;

public class NewTaskActivity extends AppCompatActivity implements PriorityDialogListener {

    private FrameLayout flContainer;
    public static final String TAG = "NewTaskActivity";
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_container);

        flContainer = findViewById(R.id.flContainer);

        if (savedInstanceState == null)
            addNewTaskFragment();
    }

    private void addNewTaskFragment() {
        getSupportFragmentManager()
                .beginTransaction()
                .add(R.id.flContainer, NewTaskFragment.newInstance(), NewTaskFragment.TAG)
                .commit();
    }


    @Override
    public void onPriorityChosen(Priority priority) {
        NewTaskFragment newTaskFragment = (NewTaskFragment)getSupportFragmentManager().findFragmentByTag(NewTaskFragment.TAG);
        newTaskFragment.onPriorityChosen(priority);
        Log.d(TAG, "Приоритет пришел, значение - " + priority.getTitle());
    }
}