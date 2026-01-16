package org.jamaica.pcog.mobile.inbox;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.support.v7.widget.Toolbar;
import android.view.MenuItem;
import android.view.View;
import android.widget.ImageButton;
import android.widget.TextView;

import com.google.gson.Gson;

import org.jamaica.pcog.mobile.R;
import org.jamaica.pcog.mobile.inbox.model.InboxModel;


public class DetailActivity extends ActionBarActivity {

    private TextView inboxTitle;
    private TextView subject;
    private TextView description;
    private TextView time;
    private TextView date;

    ImageButton img;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail);

        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        // add back arrow to toolbar
        img = (ImageButton) findViewById(R.id.backbtn);

        img.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(getApplicationContext(), InboxActivity.class);
                startActivity(intent);
                overridePendingTransition(R.anim.fade_in, R.anim.fade_out);
                finish();
            }
        });
        getSupportActionBar().setTitle("Details");

        // setting up text views and stuff
        setUpUIViews();

        // recovering data from InboxActivity, sent via intent
        Bundle bundle = getIntent().getExtras();
        if(bundle != null){
            String json = bundle.getString("InboxModel"); // getting the model from InboxActivity send via extras
            InboxModel InboxModel = new Gson().fromJson(json, InboxModel.class);

            // Then later, when you want to display image


            inboxTitle.setText(InboxModel.getTitle());
            subject.setText(InboxModel.getSubject());
            description.setText(InboxModel.getDescription());
            time.setText(InboxModel.getTime());
            date.setText(InboxModel.getDate());


            }


    }

    private void setUpUIViews() {
        inboxTitle = (TextView)findViewById(R.id.title);
        subject = (TextView) findViewById(R.id.subject) ;
        description = (TextView)findViewById(R.id.description);
        time = (TextView)findViewById(R.id.time);
        date = (TextView)findViewById(R.id.date);



    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        if (id == android.R.id.home) {
            finish();
        }

        return super.onOptionsItemSelected(item);
    }

}