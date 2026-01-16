package com.app.Regional_News;

import android.os.Bundle;
import android.view.KeyEvent;
import android.view.MenuItem;
import android.widget.ListAdapter;
import android.widget.ListView;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.app.Regional_News.adapter.NotificationAdapter;
import com.app.Regional_News.adapter.NotificationsAdapter;
import com.app.Regional_News.data.Notification_listdata;

import java.util.ArrayList;

public class NotificationActivity extends AppCompatActivity {

    private ListView listView;
    private NotificationsAdapter adapter;
    private ArrayList<Notification_listdata> notificationList;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_notification);

        // USE FOR DISPLAY SYSTEM INBUILT BACK NAVIGATION ARROW
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        // Set the toolbar title
        getSupportActionBar().setTitle(R.string.notification);

        listView = findViewById(R.id.notification_list_view);

        // Get the notifications from the intent
        notificationList = getIntent().getParcelableArrayListExtra("notifications");

        // Set the adapter
        adapter = new NotificationsAdapter(this, notificationList);
        listView.setAdapter((ListAdapter) adapter);
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