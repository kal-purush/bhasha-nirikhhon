package com.example.lexlevi.sweapp;

import android.content.Intent;
import android.support.annotation.Nullable;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.view.View;

import com.example.lexlevi.sweapp.Models.Chat;
import com.example.lexlevi.sweapp.Models.User;
import com.example.lexlevi.sweapp.Singletons.Client;
import com.example.lexlevi.sweapp.Singletons.Session;

import com.wang.avi.AVLoadingIndicatorView;
import android.content.DialogInterface;
import android.support.v7.app.AlertDialog;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.List;

import retrofit2.*;

import butterknife.ButterKnife;
import butterknife.InjectView;

public class CreateEventActivity extends AppCompatActivity {

    @InjectView(R.id.create_event_avi) AVLoadingIndicatorView _createEventAvi;
    @InjectView(R.id.create_event_inner_fab) FloatingActionButton _createEventFab;
    @InjectView(R.id.event_name) EditText _eventName;

    private String _groupId;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_create_event);
        ButterKnife.inject(this);
        _createEventAvi.hide();
        Intent intent = getIntent();
        _groupId = intent.getStringExtra("groupId");
    }
}