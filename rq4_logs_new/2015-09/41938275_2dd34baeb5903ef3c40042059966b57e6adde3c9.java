package csc4330.mike.dreamlink.activities;

import android.content.pm.ActivityInfo;
import android.os.Bundle;
import android.os.Handler;
import android.support.v7.app.ActionBarActivity;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.widget.ExpandableListView;
import android.widget.FrameLayout;

import butterknife.Bind;
import butterknife.ButterKnife;
import csc4330.mike.dreamlink.R;

/**
 * Created by Mike on 9/4/15.
 */
public class DreamFeed extends ActionBarActivity{

    @Bind(R.id.toolbar) Toolbar mainToolbar;
    @Bind(R.id.shopping_expandable_list)ExpandableListView shoppingListDisplay;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_dream_feed);
        ButterKnife.bind(this);

        setSupportActionBar(mainToolbar);
        getSupportActionBar().setTitle("Dream Feed");

    }

}