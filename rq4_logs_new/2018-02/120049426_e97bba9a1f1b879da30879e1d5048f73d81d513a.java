package io.juismi;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.AppCompatActivity;
import android.widget.TextView;

/**
 * Created by Manlio GR on 22/02/2018.
 */

public class IssueDetail extends AppCompatActivity{
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.issue_detail);
        getSupportActionBar().hide();

        TextView name = (TextView) findViewById(R.id.nameIssue);
        TextView status = (TextView) findViewById(R.id.statusIssue);
        TextView description = (TextView) findViewById(R.id.descriptionIssue);

        //name.setText(issue.getName());
        //status.setText(issue.getStatus());
        //description.setText(issue.getDescription);
    }
}