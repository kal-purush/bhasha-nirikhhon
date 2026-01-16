package com.sean.reminderapp;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.EditText;

public class EditReminderActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_reminder);

        DBHandler db = new DBHandler(this);
        Intent intent = getIntent();
        Reminder r = db.getReminder(intent.getIntExtra("TO_BE_CHANGED_ID",0));

        EditText title = (EditText) findViewById(R.id.title);
        EditText desc = (EditText) findViewById(R.id.desc);
        EditText reminderDate = (EditText) findViewById(R.id.reminderdate);
        title.setText(r.getTitle());
        desc.setText(r.getDescription());
        reminderDate.setText(r.getReminderDate());
    }

    public void onClickCancel(View v) {
        Intent intent = new Intent(this, MainActivity.class);
        startActivity(intent);
        finish();
    }

    public void onClickEditReminder(View v) {
        DBHandler db = new DBHandler(this);
        Intent intent = getIntent();
        Reminder r = db.getReminder(intent.getIntExtra("TO_BE_CHANGED_ID",0));

        EditText title = (EditText) findViewById(R.id.title);
        EditText desc = (EditText) findViewById(R.id.desc);
        EditText reminderDate = (EditText) findViewById(R.id.reminderdate);
        Reminder updated = new Reminder(r.getId(),title.getText().toString(),desc.getText().toString(),
                r.getCreationDate(),reminderDate.getText().toString());
        db.updateReminder(updated);
        intent = new Intent();
        setResult(2,intent);
        finish();
    }
}