package com.example.admin.testingv1;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import com.google.firebase.auth.FirebaseAuth;

import org.w3c.dom.Text;

public class addEvent extends AppCompatActivity implements View.OnClickListener {
    private TextView theDate;
    private Button backToProfile;
    private FirebaseAuth firebaseAuth;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_event);

        theDate = (TextView) findViewById(R.id.theDate);
        backToProfile = (Button) findViewById(R.id.backToProfile);
        firebaseAuth = FirebaseAuth.getInstance();

        Intent incomingIntent = getIntent();
        String date = incomingIntent.getStringExtra("date");

        theDate.setText(date);
        backToProfile.setOnClickListener(this);
    }

    public void onClick(View view) {
        if(view == backToProfile){
            //will open login
            startActivity(new Intent(this, ProfileActivity.class));
        }
    }
}
